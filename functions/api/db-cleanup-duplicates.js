// 안전 중복 자동 정리 — 한 번에 처리할 DB와 최대 건수 제한 가능
// ?target=hs|eng&mode=preview|execute&limit=30
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
      skipConflict.push({ key, rows });
      continue;
    }
    const sorted = rows.slice().sort((a, b) => a.created.localeCompare(b.created));
    const keep = sorted[sorted.length - 1];
    const remove = sorted.slice(0, -1);
    for (const r of remove) {
      toArchive.push({ key, id: r.id, keptId: keep.id });
    }
  }

  return { toArchive, skipConflict, queryPagesUsed: pageCount };
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
  const mode = url.searchParams.get('mode');
  const target = url.searchParams.get('target');
  const limit = parseInt(url.searchParams.get('limit') || '30');

  if (mode !== 'preview' && mode !== 'execute') {
    return new Response(JSON.stringify({
      error: 'mode=preview|execute 필요',
      usage: '?target=hs|eng&mode=preview|execute&limit=30',
      example_preview: '?target=hs&mode=preview',
      example_execute: '?target=hs&mode=execute&limit=30',
    }), {
      status: 400,
      headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
    });
  }
  if (target !== 'hs' && target !== 'eng') {
    return new Response(JSON.stringify({
      error: 'target=hs 또는 target=eng 필요'
    }), {
      status: 400,
      headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
    });
  }

  const token = context.env.NOTION_TOKEN;
  const dbId = target === 'hs' ? context.env.NOTION_HS_DB : context.env.NOTION_ENG_DB;
  const propName = target === 'hs' ? 'HS번호' : '품명(영문)';

  try {
    // Database 조회
    const dbRes = await fetch(`https://api.notion.com/v1/databases/${dbId}`, {
      method: 'GET',
      headers: { 'Authorization': `Bearer ${token}`, 'Notion-Version': NOTION_VERSION },
    });
    const dbData = await dbRes.json();
    const dataSources = dbData.data_sources || [];

    if (dataSources.length === 0) {
      throw new Error('Data source 없음');
    }

    // 첫 번째 data source만 처리 (주 data source)
    const ds = dataSources[0];
    const { toArchive, skipConflict, queryPagesUsed } = await findDuplicates(ds.id, token, propName);

    // subrequest 예산 계산: 이미 queryPagesUsed개 사용, 1개는 DB 조회
    // 남은 예산 = 50 - queryPagesUsed - 1 = 약 40~42
    // 안전하게 limit 만큼만 처리
    const actualLimit = Math.min(limit, toArchive.length);
    const toProcess = toArchive.slice(0, actualLimit);

    let archivedCount = 0;
    const errors = [];

    if (mode === 'execute') {
      for (const item of toProcess) {
        try {
          await archivePage(item.id, token);
          archivedCount++;
          await sleep(350);
        } catch (e) {
          errors.push({ id: item.id, error: e.message });
        }
      }
    }

    return new Response(JSON.stringify({
      target,
      mode,
      dataSourceName: ds.name,
      totalDuplicatesFound: toArchive.length,
      conflictsSkipped: skipConflict.length,
      processedThisRun: mode === 'execute' ? archivedCount : 0,
      remainingAfterRun: toArchive.length - archivedCount,
      errors: errors.length ? errors : undefined,
      hint: (toArchive.length - archivedCount) > 0
        ? `아직 ${toArchive.length - archivedCount}건 남음. 다시 같은 URL 호출해서 이어서 처리하세요.`
        : '✅ 모든 중복 정리 완료!',
      conflictPreview: mode === 'preview' ? skipConflict.slice(0, 10) : undefined,
    }, null, 2), {
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
