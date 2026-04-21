// refresh
const NOTION_VERSION = '2025-09-03';

// Notion rate limit 대응: 요청 사이 짧은 딜레이
const sleep = (ms) => new Promise(r => setTimeout(r, ms));

async function fetchPageWithRetry(dbId, token, cursor, maxRetries = 3) {
  const body = { page_size: 100 };
  if (cursor) body.start_cursor = cursor;

  for (let attempt = 0; attempt < maxRetries; attempt++) {
    const res = await fetch(`https://api.notion.com/v1/databases/${dbId}/query`, {
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${token}`,
        'Notion-Version': NOTION_VERSION,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(body),
    });

    if (res.ok) return await res.json();

    // 429 (rate limit) 또는 5xx → 재시도
    if (res.status === 429 || res.status >= 500) {
      const retryAfter = parseInt(res.headers.get('Retry-After') || '1');
      const waitMs = Math.min(retryAfter * 1000, 5000) * (attempt + 1);
      console.warn(`Notion ${res.status} → ${waitMs}ms 대기 후 재시도 (${attempt + 1}/${maxRetries})`);
      await sleep(waitMs);
      continue;
    }

    // 그 외 에러는 즉시 throw
    const err = await res.text();
    throw new Error(`Notion API 오류: ${res.status} ${err}`);
  }
  throw new Error(`Notion API: ${maxRetries}회 재시도 후 실패`);
}

async function queryDB(dbId, token, valuePropName) {
  const map = {};
  let cursor;
  let pageCount = 0;

  while (true) {
    const data = await fetchPageWithRetry(dbId, token, cursor);
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

    // Notion rate limit (평균 3 req/sec) 대응 — 페이지 사이 350ms 대기
    await sleep(350);
  }

  console.log(`[queryDB] ${valuePropName}: ${pageCount}페이지, ${Object.keys(map).length}건`);
  return map;
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
    // ★ Promise.all 제거, 순차 실행으로 rate limit 회피
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
    return new Response(JSON.stringify({ error: e.message }), {
      status: 500,
      headers: {
        'Content-Type': 'application/json',
        'Access-Control-Allow-Origin': '*',
      },
    });
  }
}
