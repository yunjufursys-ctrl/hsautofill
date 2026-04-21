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

const CORS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'Content-Type',
  'Access-Control-Allow-Methods': 'POST, OPTIONS',
};

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

// Database에 속한 data source ID들 조회 → 첫 번째(=주 data source) 반환
async function getPrimaryDataSourceId(dbId, token) {
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
  // 신규 page는 항상 첫 번째 data source에 추가
  const primary = dataSources[0];
  console.log(`[getPrimaryDataSourceId] DB ${dbId.slice(0,8)} → 주 data source: ${primary.id.slice(0,8)} (${primary.name || 'unnamed'})`);
  return primary.id;
}

// 모든 data source query (기존 항목 조회용)
async function getExistingMap(dbId, token, valuePropName) {
  const existing = {};

  // 모든 data source 가져오기
  const res = await fetchWithRetry(`https://api.notion.com/v1/databases/${dbId}`, {
    method: 'GET',
    headers: makeHeaders(token),
  });
  if (!res.ok) throw new Error(await res.text());
  const dbData = await res.json();
  const dataSourceIds = (dbData.data_sources || []).map(d => d.id);

  for (const dsId of dataSourceIds) {
    let cursor;
    let pageCount = 0;
    while (true) {
      const body = { page_size: 100 };
      if (cursor) body.start_cursor = cursor;
      const r = await fetchWithRetry(
        `https://api.notion.com/v1/data_sources/${dsId}/query`,
        {
          method: 'POST',
          headers: makeHeaders(token),
          body: JSON.stringify(body),
        }
      );
      if (!r.ok) throw new Error(await r.text());
      const data = await r.json();
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
      await sleep(350);
    }
    console.log(`[getExistingMap] DS ${dsId.slice(0,8)}: ${pageCount}페이지 처리`);
  }

  console.log(`[getExistingMap] ${valuePropName}: 합계 ${Object.keys(existing).length}건`);
  return existing;
}

// 새 page 생성 (parent를 data_source_id로)
async function addPage(dataSourceId, token, titlePropName, valuePropName, extraProps, key, value) {
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
    body: JSON.stringify({
      parent: { type: 'data_source_id', data_source_id: dataSourceId },
      properties
    }),
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
      const hsDataSourceId = await getPrimaryDataSourceId(hsDb, token);
      const existing = await getExistingMap(hsDb, token, 'HS번호');
      const toAdd = [];
      for (const [key, data] of Object.entries(hsMap)) {
        if (existing[key]) {
          if (existing[key] !== data.hs) conflicts.push({ type: 'HS', key, oldValue: existing[key], newValue: data.hs });
        } else {
          toAdd.push([key, data]);
        }
      }
      // 동시 요청 3개로 제한 + chunk 사이 350ms 대기
      for (let i = 0; i < toAdd.length; i += 3) {
        await Promise.all(
          toAdd.slice(i, i + 3).map(([key, data]) =>
            addPage(hsDataSourceId, token, '매핑키', 'HS번호',
              { '세트코드': data.setCode, '세트색상': data.setColor }, key, data.hs)
          )
        );
        await sleep(350);
      }
      hsAdded = toAdd.length;
    }

    if (Object.keys(engMap).length > 0) {
      const engDataSourceId = await getPrimaryDataSourceId(engDb, token);
      const existing = await getExistingMap(engDb, token, '품명(영문)');
      const toAdd = [];
      for (const [key, data] of Object.entries(engMap)) {
        if (existing[key]) {
          if (existing[key] !== data.eng) conflicts.push({ type: 'ENG', key, oldValue: existing[key], newValue: data.eng });
        } else {
          toAdd.push([key, data]);
        }
      }
      for (let i = 0; i < toAdd.length; i += 3) {
        await Promise.all(
          toAdd.slice(i, i + 3).map(([key, data]) =>
            addPage(engDataSourceId, token, '매핑키', '품명(영문)',
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
    console.error('[db-write] 에러:', e.message);
    return new Response(JSON.stringify({ error: e.message }), {
      status: 500,
      headers: { 'Content-Type': 'application/json', ...CORS },
    });
  }
}
