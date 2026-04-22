// 디버그 — 전체 row 수와 누락 이유 파악
const NOTION_VERSION = '2025-09-03';
const sleep = (ms) => new Promise(r => setTimeout(r, ms));

async function countAll(dsId, token, valuePropName) {
  let cursor;
  let pageCount = 0;
  let totalRaw = 0;
  let noTitle = 0;
  let noValue = 0;
  let emptyKey = 0;
  let emptyVal = 0;
  let valid = 0;
  const uniqueKeys = new Set();
  const duplicateKeys = [];
  const sampleEmptyKey = [];
  const sampleEmptyVal = [];

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
    totalRaw += (data.results || []).length;

    for (const page of data.results) {
      const props = page.properties;
      const titleProp = Object.values(props).find(p => p.type === 'title');
      const valueProp = props[valuePropName];

      if (!titleProp) { noTitle++; continue; }
      if (!valueProp) { noValue++; continue; }

      const key = titleProp.title.map(t => t.plain_text).join('').trim();
      const val = valueProp.rich_text.map(t => t.plain_text).join('').trim();

      if (!key) {
        emptyKey++;
        if (sampleEmptyKey.length < 5) sampleEmptyKey.push({ id: page.id, val });
        continue;
      }
      if (!val) {
        emptyVal++;
        if (sampleEmptyVal.length < 5) sampleEmptyVal.push({ id: page.id, key });
        continue;
      }

      if (uniqueKeys.has(key)) {
        if (duplicateKeys.length < 20) duplicateKeys.push({ key, id: page.id, val });
      }
      uniqueKeys.add(key);
      valid++;
    }

    if (!data.has_more || !data.next_cursor) break;
    cursor = data.next_cursor;
    await sleep(350);
    if (pageCount >= 50) break;
  }

  return {
    pageCount,
    totalRaw,           // 전체 row 수 (Notion에 있는 진짜 숫자)
    noTitle,            // title 속성 없음 (이상한 페이지)
    noValue,            // value 속성 없음
    emptyKey,           // 매핑키가 빈 문자열
    emptyVal,           // 값(HS번호/품명)이 빈 문자열
    validRows: valid,   // 유효한 row 수 (매핑된 수 = 중복 포함)
    uniqueKeysCount: uniqueKeys.size,  // 중복 제거 후 map 건수
    duplicateCount: duplicateKeys.length,
    duplicateKeys,      // 중복 샘플
    sampleEmptyKey,     // 키 빈 샘플
    sampleEmptyVal,     // 값 빈 샘플
  };
}

export async function onRequest(context) {
  const token = context.env.NOTION_TOKEN;
  const hsDb  = context.env.NOTION_HS_DB;
  const engDb = context.env.NOTION_ENG_DB;

  try {
    const result = {};

    const hsDbRes = await fetch(`https://api.notion.com/v1/databases/${hsDb}`, {
      method: 'GET',
      headers: { 'Authorization': `Bearer ${token}`, 'Notion-Version': NOTION_VERSION },
    });
    const hsDbData = await hsDbRes.json();
    result.hs_dataSources = hsDbData.data_sources?.map(d => ({ id: d.id, name: d.name }));

    for (const ds of hsDbData.data_sources || []) {
      result[`hs_${ds.id.slice(0,8)}`] = await countAll(ds.id, token, 'HS번호');
    }

    const engDbRes = await fetch(`https://api.notion.com/v1/databases/${engDb}`, {
      method: 'GET',
      headers: { 'Authorization': `Bearer ${token}`, 'Notion-Version': NOTION_VERSION },
    });
    const engDbData = await engDbRes.json();
    result.eng_dataSources = engDbData.data_sources?.map(d => ({ id: d.id, name: d.name }));

    for (const ds of engDbData.data_sources || []) {
      result[`eng_${ds.id.slice(0,8)}`] = await countAll(ds.id, token, '품명(영문)');
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
