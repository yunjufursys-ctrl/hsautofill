// Notion API 2025-09-03 — multi-data-source 대응 + 디버그 로깅
const NOTION_VERSION = '2025-09-03';

const sleep = (ms) => new Promise(r => setTimeout(r, ms));

// 디버그 로그 수집기 — 응답에 같이 보내서 클라이언트에서도 볼 수 있게 함
const debugLog = [];
function dlog(msg) {
  debugLog.push(`${new Date().toISOString().slice(11,23)} ${msg}`);
  console.log(msg);
}

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
      dlog(`Notion ${res.status} → ${waitMs}ms 대기 후 재시도 (${attempt + 1}/${maxRetries})`);
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
  dlog(`[DB ${dbId.slice(0,8)}] data source ${dataSources.length}개: ${dataSources.map(d => `${d.id.slice(0,8)}(${d.name||'-'})`).join(', ')}`);
  return dataSources.map(d => d.id);
}

async function queryOneDataSource(dataSourceId, token, valuePropName) {
  const map = {};
  let cursor;
  let pageCount = 0;
  let totalRowsSeen = 0;
  let skippedNoTitle = 0;
  let skippedNoValue = 0;
  let skippedEmpty = 0;

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
    totalRowsSeen += data.results.length;

    for (const page of data.results) {
      const props = page.properties;
      const titleProp = Object.values(props).find(p => p.type === 'title');
      const valueProp = props[valuePropName];
      if (!titleProp) { skippedNoTitle++; continue; }
      if (!valueProp) { skippedNoValue++; continue; }

      const key = titleProp.title.map(t => t.plain_text).join('').trim();
      const val = valueProp.rich_text.map(t => t.plain_text).join('').trim();
      if (!key || !val) { skippedEmpty++; continue; }
      map[key] = val;
    }

    // ★ 페이지네이션 디버그 — 매 페이지마다 상세 로그
    dlog(`  [DS ${dataSourceId.slice(0,8)}] 페이지${pageCount}: rows=${data.results.length}, has_more=${data.has_more}, next_cursor=${data.next_cursor ? 'YES' : 'null'}, 누적map=${Object.keys(map).length}`);

    if (!data.has_more) {
      dlog(`  [DS ${dataSourceId.slice(0,8)}] has_more=false → 종료`);
      break;
    }
    if (!data.next_cursor) {
      dlog(`  [DS ${dataSourceId.slice(0,8)}] ⚠️ has_more=true인데 next_cursor가 null! → 종료`);
      break;
    }
    cursor = data.next_cursor;
    await sleep(350);
  }

  dlog(`[DS ${dataSourceId.slice(0,8)}] 완료: ${pageCount}페이지, ${totalRowsSeen}행 조회, map ${Object.keys(map).length}건 (skip: noTitle=${skippedNoTitle}, noValue=${skippedNoValue}, empty=${skippedEmpty})`);
  return map;
}

async function queryDB(dbId, token, valuePropName) {
  const dataSourceIds = await getDataSourceIds(dbId, token);
  const merged = {};

  for (const dsId of dataSourceIds) {
    const partial = await queryOneDataSource(dsId, token, valuePropName);
    const beforeMerge = Object.keys(merged).length;
    Object.assign(merged, partial);
    const afterMerge = Object.keys(merged).length;
    dlog(`[merge] ${beforeMerge} + ${Object.keys(partial).length} = ${afterMerge} (중복 ${beforeMerge + Object.keys(partial).length - afterMerge}건 덮어씀)`);
    await sleep(350);
  }

  dlog(`[DB ${dbId.slice(0,8)}] 최종 합계: ${Object.keys(merged).length}건`);
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

  // 디버그 로그 초기화
  debugLog.length = 0;

  try {
    dlog('===== HS DB 조회 시작 =====');
    const hs  = await queryDB(hsDb,  token, 'HS번호');
    dlog('===== 품명 DB 조회 시작 =====');
    const eng = await queryDB(engDb, token, '품명(영문)');

    return new Response(JSON.stringify({ hs, eng, _debug: debugLog }), {
      headers: {
        'Content-Type': 'application/json',
        'Access-Control-Allow-Origin': '*',
        'Cache-Control': 'no-store',
      },
    });
  } catch (e) {
    dlog(`❌ 에러: ${e.message}`);
    return new Response(JSON.stringify({ error: e.message, _debug: debugLog }), {
      status: 500,
      headers: {
        'Content-Type': 'application/json',
        'Access-Control-Allow-Origin': '*',
      },
    });
  }
}
