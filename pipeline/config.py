"""Shared constants, regexes and classification rules.

Everything here is data, not behaviour, so the stages stay readable and the
classification rules are reviewable in one place.
"""
from __future__ import annotations

import os
import re

# --- paths -----------------------------------------------------------------

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.environ.get("PIPELINE_DATA_DIR", os.path.join(ROOT, "data"))
OUT_DIR = os.environ.get("PIPELINE_OUT_DIR", os.path.join(ROOT, "out"))
DB_PATH = os.environ.get("PIPELINE_DB", os.path.join(ROOT, "targets.db"))

# --- email parsing ---------------------------------------------------------

# Liberal scanner: pulls candidates out of arbitrary cell text. Source cells
# hold things like "a@b.com:info@b.com" and "mailto:info@b.com", so we scan
# rather than match the whole cell.
EMAIL_SCAN = re.compile(r"[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}")

# Strict validator, applied after lowercasing/stripping.
EMAIL_VALID = re.compile(r"^[^@\s]+@[^@\s]+\.[a-z]{2,}$")

# --- freemail --------------------------------------------------------------

# Exact domains.
FREEMAIL_DOMAINS = {
    "gmail.com", "googlemail.com",
    "yahoo.com", "ymail.com", "rocketmail.com",
    "hotmail.com", "live.com", "msn.com",
    "outlook.com",
    "icloud.com", "me.com", "mac.com",
    "aol.com",
    "web.de",
    "mail.ru", "inbox.ru", "bk.ru", "list.ru", "internet.ru",
    "proton.me", "protonmail.com", "protonmail.ch", "pm.me",
    "orange.fr", "wanadoo.fr",
    "free.fr",
    "libero.it",
    "t-online.de",
}

# Brands that run many ccTLD variants (gmx.de/gmx.net/..., yandex.ru/.com/...,
# yahoo.co.uk, hotmail.fr, outlook.de and so on). Matched on the first label
# plus, for the well-known ones, any country suffix.
FREEMAIL_BRANDS = (
    "gmx", "yandex", "yahoo", "hotmail", "outlook", "live", "aol", "laposte",
)


def is_freemail_domain(domain: str) -> bool:
    """True when the domain is a consumer mailbox provider, not a store."""
    if domain in FREEMAIL_DOMAINS:
        return True
    first = domain.split(".", 1)[0]
    if first in FREEMAIL_BRANDS and domain.count(".") <= 2:
        return True
    return False


# --- role accounts ---------------------------------------------------------

ROLE_LOCALS = {
    "info", "contact", "sales", "support", "hello", "office", "admin",
    "kontakt", "contacto", "mail", "enquiries", "service", "post", "shop",
    "orders", "contato", "ventas", "hola", "comercial", "marketing",
}

# --- jurisdiction ----------------------------------------------------------

EU_TLDS = {
    "at", "be", "bg", "hr", "cy", "cz", "dk", "ee", "fi", "fr", "de", "gr",
    "hu", "ie", "it", "lv", "lt", "lu", "mt", "nl", "pl", "pt", "ro", "sk",
    "si", "es", "se", "eu",
}
CASL_TLDS = {"ca"}
PECR_TLDS = {"uk"}
PERMISSIVE_TLDS = {"com", "net", "org", "us", "au", "nz", "shop", "store", "co"}

EMAILABLE_JURISDICTIONS = {"PERMISSIVE", "PECR"}


def jurisdiction_for_tld(tld: str) -> str:
    """Map a TLD to the outreach regime that governs cold email to it.

    Uses the last label, which keeps multi-part suffixes correct:
    .com.au -> au -> PERMISSIVE, .co.uk -> uk -> PECR.
    """
    if tld in EU_TLDS:
        return "EU"
    if tld in CASL_TLDS:
        return "CASL"
    if tld in PECR_TLDS:
        return "PECR"
    if tld in PERMISSIVE_TLDS:
        return "PERMISSIVE"
    return "OTHER"


# --- platform fingerprints -------------------------------------------------

# (platform, body regex, header predicates). Checked in order, so the more
# specific fingerprints come first. Regexes rather than substrings because
# naive needles misfire badly -- a bare "mage/" matches every "image/..." URL
# on the page, and "wix.com" matches any site merely linking to Wix.
PLATFORM_SIGNATURES = [
    ("Shopify",
     re.compile(r"cdn\.shopify\.com|/cdn/shop/|Shopify\.theme|\.myshopify\.com"
                r"|shopify-features|/cdn/shopifycloud/", re.I),
     [("x-shopid", None), ("x-shopify-stage", None), ("powered-by", "shopify")]),

    ("WooCommerce",
     re.compile(r"wp-content/plugins/woocommerce|woocommerce-page|woocommerce-js"
                r"|wc-add-to-cart|wc-ajax=|class=\"[^\"]*woocommerce", re.I),
     []),

    ("BigCommerce",
     re.compile(r"cdn11\.bigcommerce\.com|bigcommerce\.com/s-|bigcommerce\.com/shared"
                r"|stencil-utils", re.I),
     [("x-bc-", None)]),

    ("Magento",
     re.compile(r"Magento_|/static/version\d|data-mage-init|mage/cookies|mage-init"
                r"|/pub/static/frontend/|Mage\.Cookies", re.I),
     [("x-magento-", None)]),

    ("PrestaShop",
     re.compile(r"prestashop|/modules/ps_|var prestashop", re.I),
     [("powered-by", "prestashop")]),

    ("Wix",
     re.compile(r"_wixCssVars|static\.parastorage\.com|wixstatic\.com"
                r"|wix-(?:dropdown|image|code)|X-Wix-", re.I),
     [("x-wix-request-id", None), ("x-wix-", None)]),

    ("Squarespace",
     re.compile(r"static1\.squarespace\.com|squarespace\.com/universal"
                r"|Squarespace\.afterBodyLoad|assets\.squarespace\.com", re.I),
     [("x-servedby", "squarespace")]),

    ("OpenCart",
     re.compile(r"catalog/view/theme|index\.php\?route=common|route=checkout/cart", re.I),
     []),
]

# Platforms our product fits best; drives the biggest scoring weight.
PLATFORM_TIERS = {
    "Shopify": 35,
    "WooCommerce": 35,
    "BigCommerce": 22,
    "Magento": 22,
    "PrestaShop": 22,
    "OpenCart": 22,
    "Wix": 10,
    "Squarespace": 10,
}

# --- page signals ----------------------------------------------------------

CART_PATTERNS = [
    re.compile(r"""(?:href|action)\s*=\s*["'][^"']*/(cart|checkout|basket|panier)\b""", re.I),
    re.compile(r"""["'](?:/)?(?:cart|checkout|basket|panier)(?:/|["'?#])""", re.I),
    re.compile(r"add[-_ ]?to[-_ ]?cart", re.I),
]

PRODUCT_SCHEMA_PATTERNS = [
    re.compile(r'"@type"\s*:\s*"(?:Product|Offer|AggregateOffer)"', re.I),
    re.compile(r'"@type"\s*:\s*\[[^\]]*"(?:Product|Offer)"', re.I),
    re.compile(r'itemtype\s*=\s*["\'][^"\']*schema\.org/(?:Product|Offer)', re.I),
]

PARKED_PATTERNS = [
    re.compile(r"\b(this )?domain (is )?(for sale|is available|parked)\b", re.I),
    re.compile(r"\bbuy this domain\b", re.I),
    re.compile(r"\bdomain (name )?parking\b", re.I),
    re.compile(r"\bcoming soon\b.{0,80}\b(domain|website)\b", re.I),
    re.compile(r"\b(sedoparking|parkingcrew|bodis|afternic|dan\.com|hugedomains|namecheap parking)\b", re.I),
    re.compile(r"\bunder construction\b", re.I),
    re.compile(r"\bdefault web site page\b", re.I),
    re.compile(r"\bapache2? (ubuntu|debian) default page\b", re.I),
    re.compile(r"\bwelcome to nginx\b", re.I),
    re.compile(r"\bindex of /\b", re.I),
]

TITLE_RE = re.compile(r"<title[^>]*>(.*?)</title>", re.I | re.S)
HTML_LANG_RE = re.compile(r"<html[^>]*\slang\s*=\s*[\"']?([A-Za-z\-]{2,10})", re.I)

# --- http ------------------------------------------------------------------

CONTACT_URL = os.environ.get("CRAWLER_CONTACT_URL", "https://example.com/crawler")
USER_AGENT = os.environ.get(
    "CRAWLER_USER_AGENT",
    f"watermark-pro-lite-prospector/1.0 (+{CONTACT_URL})",
)

MAX_BODY_BYTES = 200 * 1024

# --- statuses --------------------------------------------------------------

STATUS_PENDING = "pending"
# Freemail contacts have no store site of their own to check, so they never
# enter the DNS/HTTP stages -- they exist only as paid-audience seeds.
STATUS_FREEMAIL = "freemail"
STATUS_DEAD = "dead"
STATUS_LIVE = "live"
STATUS_PARKED = "parked"
STATUS_ERROR = "error"
# Mail exchanger but no A record: reachable by email, but there is no site to
# fingerprint, so it can never be qualified as a live store.
STATUS_MX_ONLY = "mx_only"
# robots.txt forbids the site root, so we never fetched it.
STATUS_BLOCKED = "blocked"
# Older than --min-year: deliberately not crawled, held for the nurture tier.
STATUS_AGED_OUT = "aged_out"
