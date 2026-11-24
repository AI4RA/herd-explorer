// Shared header and footer. Every page calls renderLayout(activePage).
import { fillMeta } from './meta.js';
import { readKeyFromURL } from './llm.js';
import { includeRemoved, setIncludeRemoved } from './db.js';

export const REPO = 'https://github.com/AI4RA/herd-explorer';
export const FORUM = 'https://groups.ai4ra.uidaho.edu/g/main';

const PAGES = [
  ['index.html', 'Trends'],
  ['compare.html', 'Compare'],
  ['explore.html', 'Explore'],
  ['ask.html', 'Ask'],
  ['data.html', 'About HERD'],
  ['model.html', 'About the model'],
];

export const DISCLAIMER =
  'HERD Explorer is an independent project. It is not an official product of NCSES or the U.S. National Science ' +
  'Foundation, and neither NCSES, NSF nor the University of Idaho endorses it. The data, charts and projections are ' +
  'provided as is, without warranty of any kind, and are not financial, budget, legal or investment advice. ' +
  'Check figures against the official NCSES tables before relying on them.';

// Visit counts with GoatCounter (https://herd-explorer.goatcounter.com): no cookies, no IP addresses or personal
// data. It is sent the page's path only, never the query string or anchor, so nothing passed in a link (such as
// ?key=) reaches it. The script is GoatCounter's versioned count.v5.js, pinned by its hash.
const GOATCOUNTER = 'https://herd-explorer.goatcounter.com/count';
const GOATCOUNTER_JS = 'https://gc.zgo.at/count.v5.js';
const GOATCOUNTER_SRI = 'sha384-atnOLvQb9t+jTSipvd75X2yginT4PjVbqDdlJAmxMm+wYElFmeR6EmLP5bYeoRVQ';
function countVisit() {
  window.goatcounter = { path: () => location.pathname };
  const s = document.createElement('script');
  s.async = true;
  s.src = GOATCOUNTER_JS;
  s.integrity = GOATCOUNTER_SRI;
  s.crossOrigin = 'anonymous';
  s.dataset.goatcounter = GOATCOUNTER;
  document.head.appendChild(s);
}

export function renderLayout(active) {
  // A ?key= link works on any page: the key is kept for this tab and stripped from the address bar
  readKeyFromURL();
  // Counted after the key is stripped, and with the path only
  countVisit();
  const on = includeRemoved();
  const nav = PAGES.map(([href, label]) =>
    `<a href="${href}"${href === active ? ' aria-current="page"' : ''}>${label}</a>`).join('');
  document.body.insertAdjacentHTML('afterbegin', `
    <header class="site-header">
      <a class="brand" href="index.html">HERD Explorer</a>
      <nav>${nav}</nav>
    </header>${on ? `
    <div class="removed-banner" role="status">
      <p><strong>Including rows NCSES removed on the Trends, Compare and Explore tabs.</strong> ${['index.html', 'compare.html', 'explore.html'].includes(active)
        ? 'This page adds back values NCSES published in an earlier release and no longer publishes, so totals will not match NCSES\'s published tables. Projections use NCSES\'s current data only.'
        : 'This page uses NCSES\'s current data.'} <a href="data.html#removed">See what NCSES removed</a>.</p>
      <button type="button" id="removed-off">Turn off</button>
    </div>` : ''}`);
  // The switch itself is on About HERD (data.html#removed); the banner shows on every page while it is on
  document.getElementById('removed-off')?.addEventListener('click', () => { setIncludeRemoved(false); location.reload(); });
  document.body.insertAdjacentHTML('beforeend', `
    <footer class="site-footer">
      <p>Data: <a href="https://ncses.nsf.gov/explore-data/microdata/higher-education-research-development">NCSES
        Higher Education Research and Development Survey</a>, FY <span data-meta="first_year">2012</span> to FY <span data-meta="last_year">2024</span>. See <a href="data.html">About HERD</a>.</p>
      <p><a href="${REPO}">Source code on GitHub</a>
        &middot; <a href="${REPO}/blob/main/LICENSE">Apache License 2.0</a>
        &middot; Copyright 2026 Nathan Layman, University of Idaho
        &middot; <a href="${REPO}/blob/main/NOTICE">NOTICE</a></p>
      <p>Tell us what you think, report a data error, or suggest other data or a new feature: join the <a href="${FORUM}">AI4RA Groups.io listserv</a> and reply to the <a href="${FORUM}/search?q=%23DataScience&amp;ct=1">#DataScience</a> thread, or start a new topic with #DataScience in the subject line.</p>
      <p>A project of <a href="https://ai4ra.uidaho.edu">AI4RA</a> at the University of Idaho, funded by the U.S. National Science Foundation
        GRANTED program, award <a href="https://www.nsf.gov/awardsearch/show-award?AWD_ID=2427549">#2427549</a>.</p>
      <p>We count visits with <a href="https://www.goatcounter.com">GoatCounter</a>, which uses no cookies and stores no IP
        addresses or personal data, only totals such as page views, browser, country and referring site.</p>
      <p class="disclaimer">${DISCLAIMER}</p>
    </footer>`);
  fillMeta();
}
