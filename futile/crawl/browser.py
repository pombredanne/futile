# credits: https://intoli.com/blog/making-chrome-headless-undetectable/

import pyppeteer as pp
from futile.http import get_random_desktop_ua

HIDE_SCRIPTS = dict(
    hide_webdriver="""
() => {
    Object.defineProperty(navigator, 'webdriver', {
      get: () => false,
    });
  }
""",
    hide_navigator="""
() => {
    // We can mock this in as much depth as we need for the test.
    window.navigator.chrome = {
      app: {
        isInstalled: false,
      },
      webstore: {
        onInstallStageChanged: {},
        onDownloadProgress: {},
      },
      runtime: {
        PlatformOs: {
          MAC: 'mac',
          WIN: 'win',
          ANDROID: 'android',
          CROS: 'cros',
          LINUX: 'linux',
          OPENBSD: 'openbsd',
        },
        PlatformArch: {
          ARM: 'arm',
          X86_32: 'x86-32',
          X86_64: 'x86-64',
        },
        PlatformNaclArch: {
          ARM: 'arm',
          X86_32: 'x86-32',
          X86_64: 'x86-64',
        },
        RequestUpdateCheckStatus: {
          THROTTLED: 'throttled',
          NO_UPDATE: 'no_update',
          UPDATE_AVAILABLE: 'update_available',
        },
        OnInstalledReason: {
          INSTALL: 'install',
          UPDATE: 'update',
          CHROME_UPDATE: 'chrome_update',
          SHARED_MODULE_UPDATE: 'shared_module_update',
        },
        OnRestartRequiredReason: {
          APP_UPDATE: 'app_update',
          OS_UPDATE: 'os_update',
          PERIODIC: 'periodic',
        },
      },
    };
  }
""",
    hide_permission="""
() => {
    const originalQuery = window.navigator.permissions.query;
    return window.navigator.permissions.query = (parameters) => (
      parameters.name === 'notifications' ?
        Promise.resolve({ state: Notification.permission }) :
        originalQuery(parameters)
    );
  }
""",
    hide_plugins_length="""
() => {
    // Overwrite the `plugins` property to use a custom getter.
    Object.defineProperty(navigator, 'plugins', {
      // This just needs to have `length > 0` for the current test,
      // but we could mock the plugins too if necessary.
      get: () => [1, 2, 3, 4, 5],
    });
  }
""",
    hide_language="""
() => {
    // Overwrite the `plugins` property to use a custom getter.
    Object.defineProperty(navigator, 'languages', {
      get: () => ['en-US', 'en'],
    });
  }
""",
    hide_webgl_renderer="""
() => {
    const getParameter = WebGLRenderingContext.getParameter;
    WebGLRenderingContext.prototype.getParameter = function(parameter) {
      // UNMASKED_VENDOR_WEBGL
      if (parameter === 37445) {
        return 'Intel Open Source Technology Center';
      }
      // UNMASKED_RENDERER_WEBGL
      if (parameter === 37446) {
        return 'Mesa DRI Intel(R) Ivybridge Mobile ';
      }

      return getParameter(parameter);
    };
}
""",
    hide_broken_image="""
() => {
    ['height', 'width'].forEach(property => {
      // store the existing descriptor
      const imageDescriptor = Object.getOwnPropertyDescriptor(HTMLImageElement.prototype, property);

      // redefine the property with a patched descriptor
      Object.defineProperty(HTMLImageElement.prototype, property, {
        ...imageDescriptor,
        get: function() {
          // return an arbitrary non-zero dimension if the image failed to load
          if (this.complete && this.naturalHeight == 0) {
            return 20;
          }
          // otherwise, return the actual dimension
          return imageDescriptor.get.apply(this);
        },
      });
  });
}
""",
    hide_modernizr="""
() => {
    // store the existing descriptor
    const elementDescriptor = Object.getOwnPropertyDescriptor(HTMLElement.prototype, 'offsetHeight');

    // redefine the property with a patched descriptor
    Object.defineProperty(HTMLDivElement.prototype, 'offsetHeight', {
      ...elementDescriptor,
      get: function() {
        if (this.id === 'modernizr') {
            return 1;
        }
        return elementDescriptor.get.apply(this);
      },
    });
}
""",
)


async def get_headless_page(*args, **kwargs):
    """
    生成一个无法检测的浏览器页面
    """
    browser = await pp.launch(*args, **kwargs)
    page = await browser.newPage()
    await page.setUserAgent(get_random_desktop_ua())
    for script in HIDE_SCRIPTS.values():
        await page.evaluateOnNewDocument(script)

    return page
