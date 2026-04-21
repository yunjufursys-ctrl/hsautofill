// 디버그 전용 — Notion API 원본 응답을 그대로 반환
const NOTION_VERSION = '2025-09-03';

async function getDataSourceIds(dbId, token) {
  const res = await fetch(`https://api.notion.com/v1/databases/${dbId}`, {
    method: 'GET',
    headers: {
      'Authorization': `Bearer ${token}`,
      'Notion-Version': NOTION_VERSION,
    },
  });
  const data = await res.json();
  return { dbResponse: data, dataSourceIds: (data.data_sources || []).map(d => d.id) };
}

async function queryDataSourceAllPages(dsId, token) {
  const pages = [];
  let cursor;
  let pageNum = 0;

  while (true) {
    pageNum++;
    const body = { page_size: 100 };
    if (cursor) body.start_cursor = cursor;

    const res = await fetch(`https://api.notion.com/v1/data_sources/${dsId}/query`, {
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${token}`,
        'Notion-Version': NOTION_VERSION,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(body),
    });

    const data = await res.json();

    // 각 페이지의 메타 정보만 수집 (results 전체는 너무 크니 일부만)
    pages.push({
      pageNum,
      status: res.status,
      resultsCount: (data.results || []).length,
      has_more: data.has_more,
      next_cursor: data.next_cursor,
      // 디버깅용: 각 페이지의 첫/마지막 row의 created_time을 뽑아서 시간순 분포 확인
      firstRowCreated: data.results?.[0]?.created_time,
      lastRowCreated: data.results?.[data.results.length - 1]?.created_time,
      firstRowId: data.results?.[0]?.id,
      lastRowId: data.results?.[data.results.length - 1]?.id,
      // 응답 전체의 구조 확인용
      responseKeys: Object.keys(data),
      // 만약 에러가 있으면
      errorCode: data.code,
      errorMessage: data.message,
    });

    if (!data.has_more || !data.next_cursor) break;
    if (pageNum >= 20) break;  // 무한루프 방지
    cursor = data.next_cursor;
    await new Promise(r => setTimeout(r, 350));
  }

  return pages;
}

export async function onRequest(context) {
  const token = context.env.NOTION_TOKEN;
  const hsDb  = context.env.NOTION_HS_DB;
  const engDb = context.env.NOTION_ENG_DB;

  try {
    const result = {};

    const hsInfo = await getDataSourceIds(hsDb, token);
    result.hsDb = {
      dataSources: hsInfo.dbResponse.data_sources,
      dbObject: hsInfo.dbResponse.object,
      dbTitle: hsInfo.dbResponse.title,
    };

    for (const dsId of hsInfo.dataSourceIds) {
      result[`hs_ds_${dsId.slice(0,8)}`] = await queryDataSourceAllPages(dsId, token);
    }

    const engInfo = await getDataSourceIds(engDb, token);
    result.engDb = {
      dataSources: engInfo.dbResponse.data_sources,
      dbObject: engInfo.dbResponse.object,
      dbTitle: engInfo.dbResponse.title,
    };

    for (const dsId of engInfo.dataSourceIds) {
      result[`eng_ds_${dsId.slice(0,8)}`] = await queryDataSourceAllPages(dsId, token);
    }

    return new Response(JSON.stringify(result, null, 2), {
      headers: {
        'Content-Type': 'application/json',
        'Access-Control-Allow-Origin': '*',
        'Cache-Control': 'no-store',
      },
    });
  } catch (e) {
    return new Response(JSON.stringify({ error: e.message, stack: e.stack }), {
      status: 500,
      headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
    });
  }
}
