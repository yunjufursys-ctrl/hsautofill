// 디버그 — view 필터 우회 시도
const NOTION_VERSION = '2025-09-03';

export async function onRequest(context) {
  const token = context.env.NOTION_TOKEN;
  const engDb = context.env.NOTION_ENG_DB;

  try {
    // 1) database 조회 → data source ID 얻기
    const dbRes = await fetch(`https://api.notion.com/v1/databases/${engDb}`, {
      method: 'GET',
      headers: {
        'Authorization': `Bearer ${token}`,
        'Notion-Version': NOTION_VERSION,
      },
    });
    const dbData = await dbRes.json();
    const dsId = dbData.data_sources[0].id;

    // 2) 테스트 1: 일반 query
    const normalRes = await fetch(`https://api.notion.com/v1/data_sources/${dsId}/query`, {
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${token}`,
        'Notion-Version': NOTION_VERSION,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({ page_size: 100 }),
    });
    const normalData = await normalRes.json();

    // 3) 테스트 2: 빈 filter 명시
    const emptyFilterRes = await fetch(`https://api.notion.com/v1/data_sources/${dsId}/query`, {
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${token}`,
        'Notion-Version': NOTION_VERSION,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({ 
        page_size: 100,
        filter: { and: [] }
      }),
    });
    const emptyFilterData = await emptyFilterRes.json();

    // 4) 테스트 3: 오래된 row 찾는 필터 (생성 일시 기준 오름차순)
    const sortedRes = await fetch(`https://api.notion.com/v1/data_sources/${dsId}/query`, {
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${token}`,
        'Notion-Version': NOTION_VERSION,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({ 
        page_size: 10,
        sorts: [{ timestamp: 'created_time', direction: 'ascending' }]
      }),
    });
    const sortedData = await sortedRes.json();

    // 5) 테스트 4: 단품코드 = "CAB0083TN" 인 것 필터
    const textFilterRes = await fetch(`https://api.notion.com/v1/data_sources/${dsId}/query`, {
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${token}`,
        'Notion-Version': NOTION_VERSION,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({ 
        page_size: 10,
        filter: {
          property: '단품코드',
          rich_text: { equals: 'CAB0083TN' }
        }
      }),
    });
    const textFilterData = await textFilterRes.json();

    return new Response(JSON.stringify({
      dataSourceId: dsId,
      test1_normal: {
        count: normalData.results?.length,
        has_more: normalData.has_more,
        firstRowCreated: normalData.results?.[0]?.created_time,
        lastRowCreated: normalData.results?.[normalData.results.length-1]?.created_time,
        error: normalData.code,
      },
      test2_emptyFilter: {
        count: emptyFilterData.results?.length,
        has_more: emptyFilterData.has_more,
        firstRowCreated: emptyFilterData.results?.[0]?.created_time,
        error: emptyFilterData.code,
        errorMsg: emptyFilterData.message,
      },
      test3_sortedAscending: {
        count: sortedData.results?.length,
        firstRows: sortedData.results?.slice(0, 5).map(p => ({
          id: p.id,
          created: p.created_time,
        })),
        error: sortedData.code,
        errorMsg: sortedData.message,
      },
      test4_filterByCAB0083TN: {
        count: textFilterData.results?.length,
        has_more: textFilterData.has_more,
        results: textFilterData.results?.map(p => ({
          id: p.id,
          created: p.created_time,
        })),
        error: textFilterData.code,
        errorMsg: textFilterData.message,
      },
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
