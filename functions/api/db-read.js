// Notion API 2025-09-03 — multi-data-source 대응
const NOTION_VERSION = '2025-09-03';

const sleep = (ms) => new Promise(r => setTimeout(r, ms));

function makeHeaders(token) {
  return {
    'Authorization': `Bearer ${token}`,
    'Notion-Version': NOTION_VERSION,
    'Content-Type': 'application/json',
  };
}

// 429/5xx 자동 재시도 fetch
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

// Database에 속한 data source ID들 자동 조회
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
  console.log(`[getDataSourceIds] DB ${dbId.slice(0,8)} → data source ${dataSources.length}개:`,
    dataSources.map(d => `${d.id.slice(0,8)} (${d.name || 'unnamed'})`).join(', '));
  return dataSources.map(d => d.id);
}

// 단일 data source query
async function queryOneDataSource(dataSourceId, token, valuePropName) {
  const map = {};
  let cursor;
  let pageCount = 0;

  while (true) {
    const body = { page_size: 100 };
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

    if (!data.has_more) break;
    cursor = data.next_cursor;
    await sleep(350);  // rate limit 회피
  }

  console.log(`[queryOneDataSource] DS ${dataSourceId.slice(0,8)}: ${pageCount}페이지, ${Object.keys(map).length}건`);
  return map;
}

// Database 전체 query (모든 data source 합침)
async function queryDB(dbId, token, valuePropName) {
  const dataSourceIds = await getDataSourceIds(dbId, token);
  const merged = {};

  for (const dsId of dataSourceIds) {
    const partial = await queryOneDataSource(dsId, token, valuePropName);
    Object.assign(merged, partial);
    await sleep(350);
  }

  console.log(`[queryDB] DB ${dbId.slice(0,8)} 합계: ${Object.keys(merged).length}건`);
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
    // 순차 실행으로 rate limit 회피
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
