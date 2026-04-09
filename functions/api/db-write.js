// Cloudflare Pages Function: Notion DB에 매핑 데이터 저장
const NOTION_VERSION = '2022-06-28';

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

async function getExistingMap(dbId, token, valuePropName) {
  const existing = {};
  let cursor;
  while (true) {
    const body = { page_size: 100 };
    if (cursor) body.start_cursor = cursor;
    const res = await fetch(`https://api.notion.com/v1/databases/${dbId}/query`, {
      method: 'POST',
      headers: makeHeaders(token),
      body: JSON.stringify(body),
    });
    if (!res.ok) throw new Error(await res.text());
    const data = await res.json();
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
  }
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
  const res = await fetch('https://api.notion.com/v1/pages', {
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
      for (let i = 0; i < toAdd.length; i += 10) {
        await Promise.all(
          toAdd.slice(i, i + 10).map(([key, data]) =>
            addPage(hsDb, token, '매핑키', 'HS번호',
              { '세트코드': data.setCode, '세트색상': data.setColor }, key, data.hs)
          )
        );
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
      for (let i = 0; i < toAdd.length; i += 10) {
        await Promise.all(
          toAdd.slice(i, i + 10).map(([key, data]) =>
            addPage(engDb, token, '매핑키', '품명(영문)',
              { '단품코드': data.itemCode, '단품색상': data.itemColor }, key, data.eng)
          )
        );
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
