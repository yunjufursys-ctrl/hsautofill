// 디버그 전용 — 특정 페이지 ID를 직접 조회
const NOTION_VERSION = '2025-09-03';

export async function onRequest(context) {
  const token = context.env.NOTION_TOKEN;
  const url = new URL(context.request.url);
  const pageId = url.searchParams.get('id');

  if (!pageId) {
    return new Response(JSON.stringify({ error: 'id 파라미터 필요' }), {
      status: 400,
      headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
    });
  }

  try {
    const res = await fetch(`https://api.notion.com/v1/pages/${pageId}`, {
      method: 'GET',
      headers: {
        'Authorization': `Bearer ${token}`,
        'Notion-Version': NOTION_VERSION,
      },
    });
    const data = await res.json();

    return new Response(JSON.stringify({
      status: res.status,
      id: data.id,
      created_time: data.created_time,
      archived: data.archived,
      in_trash: data.in_trash,
      parent: data.parent,
      properties_keys: data.properties ? Object.keys(data.properties) : null,
      error_code: data.code,
      error_message: data.message,
      raw: data,
    }, null, 2), {
      headers: {
        'Content-Type': 'application/json',
        'Access-Control-Allow-Origin': '*',
        'Cache-Control': 'no-store',
      },
    });
  } catch (e) {
    return new Response(JSON.stringify({ error: e.message }), {
      status: 500,
      headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
    });
  }
}
