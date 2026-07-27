import { useEffect } from 'react';
import { useLang } from '@rspress/core/runtime';

const labels = {
  en: {
    menu: 'Open navigation menu',
    closeMenu: 'Close navigation menu',
    navigation: 'Navigation menu',
    search: 'Search documentation',
    homeTitle: 'queuebit documentation'
  },
  zh: {
    menu: '打开导航菜单',
    closeMenu: '关闭导航菜单',
    navigation: '导航菜单',
    search: '搜索文档',
    homeTitle: 'queuebit 文档'
  }
};

function applyLabels() {
  const language = document.documentElement.lang.startsWith('zh') ? 'zh' : 'en';
  const current = labels[language];

  const navScreen = document.querySelector<HTMLElement>('.rp-nav-screen');
  if (navScreen) {
    navScreen.id = 'queuebit-nav-screen';
    navScreen.setAttribute('aria-label', current.navigation);
  }

  document.querySelectorAll<HTMLElement>('.rp-nav-hamburger').forEach((element) => {
    const isMobile = element.classList.contains('rp-nav-hamburger__sm');
    const isOpen = isMobile
      && element.classList.contains('rp-nav-hamburger--active')
      && Boolean(navScreen?.classList.contains('rp-nav-screen--open'));
    element.setAttribute('aria-label', isOpen ? current.closeMenu : current.menu);
    if (isMobile) {
      if (navScreen) {
        element.setAttribute('aria-controls', navScreen.id);
      } else {
        element.removeAttribute('aria-controls');
      }
      element.setAttribute('aria-expanded', String(isOpen));
    }
  });
  document
    .querySelectorAll<HTMLElement>('.rp-search-panel__input')
    .forEach((element) => element.setAttribute('aria-label', current.search));

  const docsSidebar = document.querySelector<HTMLElement>('.rp-doc-layout__sidebar');
  const docsMenu = document.querySelector<HTMLElement>('.rp-sidebar-menu__left');
  if (docsSidebar && docsMenu) {
    docsSidebar.id = 'queuebit-docs-sidebar';
    docsMenu.setAttribute('aria-controls', docsSidebar.id);
    docsMenu.setAttribute(
      'aria-expanded',
      String(docsSidebar.classList.contains('rp-doc-layout__sidebar--open'))
    );
  }

  document.querySelectorAll<HTMLElement>('footer').forEach((footer) => {
    if (footer.textContent?.includes('Apache-2.0')) {
      const message = language === 'zh'
        ? '基于 Apache-2.0 许可证发布。'
        : 'Released under the Apache-2.0 License.';
      if (footer.textContent.trim() !== message) footer.textContent = message;
    }
  });

  const isHome = window.location.pathname.endsWith('/queuebit/')
    || window.location.pathname.endsWith('/queuebit/zh/');
  const hiddenHomeHeading = document.querySelector<HTMLHeadingElement>('.qb-home-sr-title');
  if (!isHome) {
    hiddenHomeHeading?.remove();
    return;
  }

  if (hiddenHomeHeading || !document.querySelector('h1')) {
    let heading = hiddenHomeHeading;
    if (!heading) {
      heading = document.createElement('h1');
      heading.className = 'qb-sr-only qb-home-sr-title';
      const main = document.querySelector('main') || document.body;
      main.prepend(heading);
    }
    heading.textContent = current.homeTitle;
  }

  document
    .querySelectorAll<HTMLElement>('article.rp-home-feature__card--clickable')
    .forEach((card) => {
      card.setAttribute('role', 'link');
      card.tabIndex = 0;
      const title = card.querySelector('h2, h3, strong')?.textContent?.trim();
      if (title) card.setAttribute('aria-label', title);
      if (card.dataset.queuebitKeyboard === 'true') return;
      card.dataset.queuebitKeyboard = 'true';
      card.addEventListener('keydown', (event) => {
        if (event.key === 'Enter' || event.key === ' ') {
          event.preventDefault();
          event.stopPropagation();
          card.click();
        }
      });
    });
}

export default function A11yLabels() {
  const rspressLang = useLang();
  const language = rspressLang.startsWith('zh') ? 'zh' : 'en';

  useEffect(() => {
    applyLabels();
    const observer = new MutationObserver(applyLabels);
    observer.observe(document.body, {
      childList: true,
      subtree: true,
      attributes: true,
      attributeFilter: ['class']
    });
    const closeOpenSidebar = (event: KeyboardEvent) => {
      if (event.key !== 'Escape') return;
      const navScreen = document.querySelector('.rp-nav-screen--open');
      const navMenu = document.querySelector<HTMLElement>(
        '.rp-nav-hamburger__sm.rp-nav-hamburger--active'
      );
      if (navScreen && navMenu) {
        event.preventDefault();
        event.stopPropagation();
        event.stopImmediatePropagation();
        navMenu.click();
        requestAnimationFrame(() => navMenu.focus());
        return;
      }

      const sidebar = document.querySelector('.rp-doc-layout__sidebar--open');
      const menu = document.querySelector<HTMLElement>('.rp-sidebar-menu__left');
      const mask = document.querySelector<HTMLElement>('.rp-sidebar-menu__mask');
      if (!sidebar || !menu || !mask) return;
      event.preventDefault();
      event.stopPropagation();
      event.stopImmediatePropagation();
      mask.click();
      menu.focus();
    };
    document.addEventListener('keydown', closeOpenSidebar, true);
    return () => {
      observer.disconnect();
      document.removeEventListener('keydown', closeOpenSidebar, true);
    };
  }, [language]);

  const copy = language === 'zh'
    ? {
        title: 'v0.1 预览文档',
        detail: '本站描述 Queuebit v0.1 计划稳定的使用方式；当前安装版本请以 npm 包和发布说明为准。'
      }
    : {
        title: 'v0.1 preview documentation',
        detail: 'This site describes the intended stable Queuebit v0.1 usage; check the installed npm package and release notes for current availability.'
      };

  return (
    <aside className="qb-release-banner" role="status" aria-live="polite">
      <strong>{copy.title}</strong>
      <span>{copy.detail}</span>
    </aside>
  );
}
