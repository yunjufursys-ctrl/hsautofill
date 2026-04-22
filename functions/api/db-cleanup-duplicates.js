// 안전 중복 자동 정리 — 같은 값으로 중복된 row 중 최신 것만 남기고 archive
const NOTION_VERSION = '2025-09-03';
const sleep = (ms) => new Promise(r => setTimeout(r, ms));

async function findDuplicates(dsId, token, valuePropName) {
  const keyToRows = {};
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
      keyToRows[key].push({ id: page.id, val, created: page.created_time });
    }

    if (!data.has_more || !data.next_cursor) break;
    cursor = data.next_cursor;
    await sleep(350);
    if (pageCount >= 50) break;
  }

  const toArchive = [];
  const skipConflict = [];

  for (const [key, rows] of Object.entries(keyToRows)) {
    if (rows.length <= 1) continue;
    const values = new Set(rows.map(r => r.val));
    if (values.size > 1) {
      // 값이 다른 중복 → 스킵 (사람이 수동으로 처리)
      skipConflict.push({ key, rows });
      continue;
    }
    // 값이 같은 중복 → 가장 최근 것만 남기고 나머지 archive
    const sorted = rows.slice().sort((a, b) => a.created.localeCompare(b.created));
    const keep = sorted[sorted.length - 1];
    const remove = sorted.slice(0, -1);
    for (const r of remove) {
      toArchive.push({ key, id: r.id, keptId: keep.id });
    }
  }

  return { toArchive, skipConflict };
}

async function archivePage(pageId, token) {
  const res = await fetch(`https://api.notion.com/v1/pages/${pageId}`, {
    method: 'PATCH',
    headers: {
      'Authorization': `Bearer ${token}`,
      'Notion-Version': NOTION_VERSION,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({ archived: true }),
  });
  if (!res.ok) {
    const err = await res.text();
    throw new Error(`Archive 실패 ${pageId}: ${res.status} ${err}`);
  }
}

export async function onRequest(context) {
  const url = new URL(context.request.url);
  const mode = url.searchParams.get('mode');  // 'preview' 또는 'execute'

  if (mode !== 'preview' && mode !== 'execute') {
    return new Response(JSON.stringify({
      error: '?mode=preview 또는 ?mode=execute 필요',
      hint: '먼저 preview로 확인 후, execute로 실제 실행하세요'
    }), {
      status: 400,
      headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
    });
  }

  const token = context.env.NOTION_TOKEN;
  const hsDb  = context.env.NOTION_HS_DB;
  const engDb = context.env.NOTION_ENG_DB;

  try {
    const result = { mode, databases: {} };

    for (const [label, dbId, propName] of [['HS', hsDb, 'HS번호'], ['ENG', engDb, '품명(영문)']]) {
      const dbRes = await fetch(`https://api.notion.com/v1/databases/${dbId}`, {
        method: 'GET',
        headers: { 'Authorization': `Bearer ${token}`, 'Notion-Version': NOTION_VERSION },
      });
      const dbData = await dbRes.json();

      const dbResult = { dataSources: {} };
      for (const ds of dbData.data_sources || []) {
        const { toArchive, skipConflict } = await findDuplicates(ds.id, token, propName);

        let archivedCount = 0;
        const errors = [];

        if (mode === 'execute') {
          // 실제 archive 실행 (3개씩 병렬 + 350ms 대기)
          for (let i = 0; i < toArchive.length; i += 3) {
            const chunk = toArchive.slice(i, i + 3);
            const results = await Promise.allSettled(
              chunk.map(item => archivePage(item.id, token))
            );
            results.forEach((r, idx) => {
              if (r.status === 'fulfilled') archivedCount++;
              else errors.push({ id: chunk[idx].id, error: r.reason.message });
            });
            await sleep(350);
          }
        }

        dbResult.dataSources[ds.id.slice(0, 8)] = {
          dataSourceName: ds.name,
          toArchiveCount: toArchive.length,
          conflictCount: skipConflict.length,
          archivedCount: mode === 'execute' ? archivedCount : null,
          errors: errors.length ? errors : undefined,
          toArchivePreview: mode === 'preview' ? toArchive.slice(0, 5) : undefined,
          conflictPreview: mode === 'preview' ? skipConflict.slice(0, 5) : undefined,
        };
      }
      result.databases[label] = dbResult;
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
