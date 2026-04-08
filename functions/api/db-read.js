// Cloudflare Pages Function: Notion DB에서 매핑 데이터 읽기
// HS번호: 세트코드|세트색상 복합키
// 품명(영문): 단품코드|단품색상 복합키

const NOTION_VERSION = '2022-06-28';

async function queryDB(dbId, token) {
  const map = {};
  let cursor;

  while (true) {
    const body = { page_size: 100 };
    if (cursor) body.start_cursor = cursor;

    const res = await fetch(`https://api.notion.com/v1/databases/${dbId}/query`, {
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${token}`,
        'Notion-Version': NOTION_VERSION,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(body),
    });

    if (!res.ok) {
      const err = await res.text();
      throw new Error(`Notion API 오류: ${res.status} ${err}`);
    }

    const data = await res.json();

    for (const page of data.results) {
      const props = page.properties;
      // 매핑키 (title) = 복합키
      const titleProp = Object.values(props).find(p => p.type === 'title');
      const textProp  = Object.values(props).find(p => p.type === 'rich_text');
      if (!titleProp || !textProp) continue;

      const key = titleProp.title.map(t => t.plain_text).join('').trim();
      const val = textProp.rich_text.map(t => t.plain_text).join('').trim();
      if (key && val) map[key] = val;
    }

    if (!data.has_more) break;
    cursor = data.next_cursor;
  }

  return map;
}

export async function onRequest(context) {
  const token  = context.env.NOTION_TOKEN;
  const hsDb   = context.env.NOTION_HS_DB;
  const engDb  = context.env.NOTION_ENG_DB;

  // CORS preflight
  if (context.request.method === 'OPTIONS') {
    return new Response(null, {
      headers: {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Headers': 'Content-Type',
      },
    });
  }

  try {
    const [hs, eng] = await Promise.all([
      queryDB(hsDb, token),
      queryDB(engDb, token),
    ]);

    return new Response(JSON.stringify({ hs, eng }), {
      headers: {
        'Content-Type': 'application/json',
        'Access-Control-Allow-Origin': '*',
      },
    });
  } catch (e) {
    return new Response(JSON.stringify({ error: e.message }), {
      status: 500,
      headers: { 'Access-Control-Allow-Origin': '*' },
    });
  }
}
