// Cloudflare Pages Function: Notion DB에 매핑 데이터 저장
const NOTION_VERSION = '2025-09-03';

const sleep = (ms) => new Promise(r => setTimeout(r, ms));

function makeHeaders(token) {
  return {
    'Authorization': `Bearer ${token}`,
    'Notion-Version': NOTION_VERSION,
    'Content-Type': 'application/json',
  };
}

const CORS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'Content-Type',
  'Access-Control-Allow-Methods': 'POST, OPTIONS',
};

// 429 / 5xx 자동 재시도가 포함된 fetch
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
    return res; // 그 외 에러는 호출자가 처리
  }
  throw new Error(`Notion API: ${maxRetries}회 재시도 후 실패`);
}

async function getExistingMap(dbId, token, valuePropName) {
  const existing = {};
  let cursor;
  let pageCount = 0;
  while (true) {
    const body = { page_size: 100 };
    if (cursor) body.start_cursor = cursor;
    const res = await fetchWithRetry(`https://api.notion.com/v1/data_sources/{id}/query`, {
      method: 'POST',
      headers: makeHeaders(token),
      body: JSON.stringify(body),
    });
    if (!res.ok) throw new Error(await res.text());
    const data = await res.json();
    pageCount++;
    for (const page of data.results) {
      const props = page.properties;
      const titleProp = Object.values(props).find(p => p.type === 'title');
      const valueProp = props[valuePropName];
      const key = titleProp?.title?.map(t => t.plain_text).join('').trim();
      const val = valueProp?.rich_text?.map(t => t.plain_text).join('').trim();
      if (key && val) existing[key] = val;
    }
    if (!data.has_more) break;
    cursor = data.next_cursor;
    // 페이지 사이 350ms 대기 — Notion rate limit 회피
    await sleep(350);
  }
  console.log(`[getExistingMap] ${valuePropName}: ${pageCount}페이지, ${Object.keys(existing).length}건`);
  return existing;
}

async function addPage(dbId, token, titlePropName, valuePropName, extraProps, key, value) {
  const properties = {
    [titlePropName]: { title: [{ text: { content: key } }] },
    [valuePropName]: { rich_text: [{ text: { content: value } }] },
  };
  for (const [propName, propValue] of Object.entries(extraProps)) {
    properties[propName] = { rich_text: [{ text: { content: propValue } }] };
  }
  const res = await fetchWithRetry('https://api.notion.com/v1/pages', {
    method: 'POST',
    headers: makeHeaders(token),
    body: JSON.stringify({ parent: { database_id: dbId }, properties }),
  });
  if (!res.ok) throw new Error(await res.text());
}

export async function onRequestOptions() {
  return new Response(null, { headers: CORS });
}

export async function onRequestPost(context) {
  const token = context.env.NOTION_TOKEN;
  const hsDb  = context.env.NOTION_HS_DB;
  const engDb = context.env.NOTION_ENG_DB;

  try {
    if (!token) throw new Error('NOTION_TOKEN 환경변수 없음');

    const body   = await context.request.json();
    const hsMap  = body.hs  || {};
    const engMap = body.eng || {};

    let hsAdded = 0, engAdded = 0;
    const conflicts = [];

    if (Object.keys(hsMap).length > 0) {
      const existing = await getExistingMap(hsDb, token, 'HS번호');
      const toAdd = [];
      for (const [key, data] of Object.entries(hsMap)) {
        if (existing[key]) {
          if (existing[key] !== data.hs) conflicts.push({ type: 'HS', key, oldValue: existing[key], newValue: data.hs });
        } else {
          toAdd.push([key, data]);
        }
      }
      // ★ 동시 요청 3개로 제한 + chunk 사이 350ms 대기
      for (let i = 0; i < toAdd.length; i += 3) {
        await Promise.all(
          toAdd.slice(i, i + 3).map(([key, data]) =>
            addPage(hsDb, token, '매핑키', 'HS번호',
              { '세트코드': data.setCode, '세트색상': data.setColor }, key, data.hs)
          )
        );
        await sleep(350);
      }
      hsAdded = toAdd.length;
    }

    if (Object.keys(engMap).length > 0) {
      const existing = await getExistingMap(engDb, token, '품명(영문)');
      const toAdd = [];
      for (const [key, data] of Object.entries(engMap)) {
        if (existing[key]) {
          if (existing[key] !== data.eng) conflicts.push({ type: 'ENG', key, oldValue: existing[key], newValue: data.eng });
        } else {
          toAdd.push([key, data]);
        }
      }
      // ★ 동시 요청 3개로 제한 + chunk 사이 350ms 대기
      for (let i = 0; i < toAdd.length; i += 3) {
        await Promise.all(
          toAdd.slice(i, i + 3).map(([key, data]) =>
            addPage(engDb, token, '매핑키', '품명(영문)',
              { '단품코드': data.itemCode, '단품색상': data.itemColor }, key, data.eng)
          )
        );
        await sleep(350);
      }
      engAdded = toAdd.length;
    }

    return new Response(JSON.stringify({ ok: true, hsAdded, engAdded, conflicts }), {
      headers: { 'Content-Type': 'application/json', ...CORS },
    });
  } catch (e) {
    return new Response(JSON.stringify({ error: e.message }), {
      status: 500,
      headers: { 'Content-Type': 'application/json', ...CORS },
    });
  }
}
