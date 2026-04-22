// 디버그 — 중복된 매핑키를 전부 찾아서 리포트
const NOTION_VERSION = '2025-09-03';
const sleep = (ms) => new Promise(r => setTimeout(r, ms));

async function findDuplicates(dsId, token, valuePropName) {
  const keyToRows = {};  // key -> [{id, val, created}]
  let cursor;
  let pageCount = 0;

  while (true) {
    const body = {
      page_size: 100,
      sorts: [{ timestamp: 'created_time', direction: 'ascending' }]
    };
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
    pageCount++;

    for (const page of data.results) {
      const props = page.properties;
      const titleProp = Object.values(props).find(p => p.type === 'title');
      const valueProp = props[valuePropName];
      if (!titleProp || !valueProp) continue;
      const key = titleProp.title.map(t => t.plain_text).join('').trim();
      const val = valueProp.rich_text.map(t => t.plain_text).join('').trim();
      if (!key || !val) continue;

      if (!keyToRows[key]) keyToRows[key] = [];
      keyToRows[key].push({
        id: page.id,
        val,
        created: page.created_time,
      });
    }

    if (!data.has_more || !data.next_cursor) break;
    cursor = data.next_cursor;
    await sleep(350);
    if (pageCount >= 50) break;
  }

  // 중복만 추출
  const duplicates = {};
  const safeDuplicates = [];   // 같은 값으로 중복 (안전하게 삭제 가능)
  const conflictDuplicates = []; // 다른 값으로 중복 (수동 확인 필요)

  for (const [key, rows] of Object.entries(keyToRows)) {
    if (rows.length > 1) {
      duplicates[key] = rows;
      const values = new Set(rows.map(r => r.val));
      if (values.size === 1) {
        safeDuplicates.push({ key, count: rows.length, val: rows[0].val, rows });
      } else {
        conflictDuplicates.push({ key, count: rows.length, values: [...values], rows });
      }
    }
  }

  return {
    totalKeys: Object.keys(keyToRows).length,
    totalRows: Object.values(keyToRows).reduce((s, r) => s + r.length, 0),
    duplicateKeyCount: Object.keys(duplicates).length,
    safeDuplicatesCount: safeDuplicates.length,
    conflictDuplicatesCount: conflictDuplicates.length,
    safeDuplicates,         // 그냥 삭제해도 안전한 중복
    conflictDuplicates,     // 값이 서로 달라서 수동 확인 필요
  };
}

export async function onRequest(context) {
  const token = context.env.NOTION_TOKEN;
  const hsDb  = context.env.NOTION_HS_DB;
  const engDb = context.env.NOTION_ENG_DB;

  try {
    const result = {};

    // HS DB
    const hsDbRes = await fetch(`https://api.notion.com/v1/databases/${hsDb}`, {
      method: 'GET',
      headers: { 'Authorization': `Bearer ${token}`, 'Notion-Version': NOTION_VERSION },
    });
    const hsDbData = await hsDbRes.json();
    for (const ds of hsDbData.data_sources || []) {
      result[`hs_${ds.id.slice(0,8)}`] = await findDuplicates(ds.id, token, 'HS번호');
    }

    // 품명 DB
    const engDbRes = await fetch(`https://api.notion.com/v1/databases/${engDb}`, {
      method: 'GET',
      headers: { 'Authorization': `Bearer ${token}`, 'Notion-Version': NOTION_VERSION },
    });
    const engDbData = await engDbRes.json();
    for (const ds of engDbData.data_sources || []) {
      result[`eng_${ds.id.slice(0,8)}`] = await findDuplicates(ds.id, token, '품명(영문)');
    }

    return new Response(JSON.stringify(result, null, 2), {
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
