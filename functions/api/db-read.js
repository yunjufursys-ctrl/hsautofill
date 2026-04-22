// Notion API 2025-09-03 — multi-data-source 대응 + 명시적 정렬로 전체 조회
const NOTION_VERSION = '2025-09-03';

const sleep = (ms) => new Promise(r => setTimeout(r, ms));

function makeHeaders(token) {
  return {
    'Authorization': `Bearer ${token}`,
    'Notion-Version': NOTION_VERSION,
    'Content-Type': 'application/json',
  };
}

async function fetchWithRetry(url, options, maxRetries = 3) {
  for (let attempt = 0; attempt < maxRetries; attempt++) {
    const res = await fetch(url, options);
    if (res.ok) return res;
    if (res.status === 429 || res.status >= 500) {
      const retryAfter = parseInt(res.headers.get('Retry-After') || '1');
      const waitMs = Math.min(retryAfter * 1000, 5000) * (attempt + 1);
      console.warn(`Notion ${res.status} → ${waitMs}ms 대기 후 재시도 (${attempt + 1}/${maxRetries})`);
      await sleep(waitMs);
      continue;
    }
    return res;
  }
  throw new Error(`Notion API: ${maxRetries}회 재시도 후 실패`);
}

async function getDataSourceIds(dbId, token) {
  const res = await fetchWithRetry(`https://api.notion.com/v1/databases/${dbId}`, {
    method: 'GET',
    headers: makeHeaders(token),
  });
  if (!res.ok) {
    const err = await res.text();
    throw new Error(`Database 조회 실패: ${res.status} ${err}`);
  }
  const data = await res.json();
  const dataSources = data.data_sources || [];
  if (dataSources.length === 0) {
    throw new Error(`Database ${dbId}에 data source 없음`);
  }
  console.log(`[DB ${dbId.slice(0,8)}] data source ${dataSources.length}개`);
  return dataSources.map(d => d.id);
}

async function queryOneDataSource(dataSourceId, token, valuePropName) {
  const map = {};
  let cursor;
  let pageCount = 0;

  while (true) {
    const body = {
      page_size: 100,
      // ★ 명시적 정렬 — 이게 있어야 전체를 순회 가능
      sorts: [{ timestamp: 'created_time', direction: 'ascending' }]
    };
    if (cursor) body.start_cursor = cursor;

    const res = await fetchWithRetry(
      `https://api.notion.com/v1/data_sources/${dataSourceId}/query`,
      {
        method: 'POST',
        headers: makeHeaders(token),
        body: JSON.stringify(body),
      }
    );
    if (!res.ok) {
      const err = await res.text();
      throw new Error(`Data source query 실패: ${res.status} ${err}`);
    }
    const data = await res.json();
    pageCount++;

    for (const page of data.results) {
      const props = page.properties;
      const titleProp = Object.values(props).find(p => p.type === 'title');
      const valueProp = props[valuePropName];
      if (!titleProp || !valueProp) continue;

      const key = titleProp.title.map(t => t.plain_text).join('').trim();
      const val = valueProp.rich_text.map(t => t.plain_text).join('').trim();
      if (key && val) map[key] = val;
    }

    if (!data.has_more || !data.next_cursor) break;
    cursor = data.next_cursor;
    await sleep(350);

    // 안전장치 — 50페이지(5000건) 이상 가면 경고
    if (pageCount >= 50) {
      console.warn(`[DS ${dataSourceId.slice(0,8)}] 50페이지 도달, 루프 중단`);
      break;
    }
  }

  console.log(`[DS ${dataSourceId.slice(0,8)}] ${pageCount}페이지, ${Object.keys(map).length}건`);
  return map;
}

async function queryDB(dbId, token, valuePropName) {
  const dataSourceIds = await getDataSourceIds(dbId, token);
  const merged = {};

  for (const dsId of dataSourceIds) {
    const partial = await queryOneDataSource(dsId, token, valuePropName);
    Object.assign(merged, partial);
    await sleep(350);
  }

  console.log(`[DB ${dbId.slice(0,8)}] 최종 ${Object.keys(merged).length}건`);
  return merged;
}

export async function onRequest(context) {
  const token = context.env.NOTION_TOKEN;
  const hsDb  = context.env.NOTION_HS_DB;
  const engDb = context.env.NOTION_ENG_DB;

  if (context.request.method === 'OPTIONS') {
    return new Response(null, {
      headers: {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Headers': 'Content-Type',
      },
    });
  }

  try {
    const hs  = await queryDB(hsDb,  token, 'HS번호');
    const eng = await queryDB(engDb, token, '품명(영문)');

    return new Response(JSON.stringify({ hs, eng }), {
      headers: {
        'Content-Type': 'application/json',
        'Access-Control-Allow-Origin': '*',
        'Cache-Control': 'no-store',
      },
    });
  } catch (e) {
    console.error('[db-read] 에러:', e.message);
    return new Response(JSON.stringify({ error: e.message }), {
      status: 500,
      headers: {
        'Content-Type': 'application/json',
        'Access-Control-Allow-Origin': '*',
      },
    });
  }
}
