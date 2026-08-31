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

# A live shop with a working cart is demonstrably a shop, whether or not its
# platform could be named. 23% of live shops could not be fingerprinted -- many
# are behind a CDN or on a custom build -- and scoring them zero on the largest
# weight sent them to the nurture pile for a gap in our knowledge rather than a
# defect in the shop. They are unknown fit, not bad fit, so they get a floor
# level with the platforms we can name but do not fit well.
PLATFORM_UNKNOWN_FIT = 10

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


# --- stage 5: outreach profiling -------------------------------------------

# LinkedIn only. Several URL shapes are in circulation -- the modern
# /company/ and /in/ paths, the legacy /pub/ profile path, and lnkd.in short
# links -- and a shop that publishes any of them publishes only one, so all of
# them have to be recognised.
LINKEDIN_PATTERNS = [
    ("personal", re.compile(
        r"""https?://(?:[a-z]{2,3}\.)?linkedin\.com/in/([A-Za-z0-9_\-.%]+)""", re.I)),
    ("personal", re.compile(
        r"""https?://(?:[a-z]{2,3}\.)?linkedin\.com/pub/([A-Za-z0-9_\-.%/]+)""", re.I)),
    ("company", re.compile(
        r"""https?://(?:[a-z]{2,3}\.)?linkedin\.com/company/([A-Za-z0-9_\-.%]+)""", re.I)),
    ("company", re.compile(
        r"""https?://(?:[a-z]{2,3}\.)?linkedin\.com/(?:showcase|school)/([A-Za-z0-9_\-.%]+)""", re.I)),
    ("short", re.compile(r"""https?://lnkd\.in/([A-Za-z0-9_\-]+)""", re.I)),
]

# Slugs that are LinkedIn's own pages or share widgets rather than an account.
LINKEDIN_NOISE = {
    "share", "sharearticle", "shareoffsite", "sharing", "cws", "feed", "login",
    "signup", "uas", "help", "legal", "company-beta", "search", "jobs",
    "pulse", "learning", "posts", "groups", "events",
}

# The legal-notice page. EU e-commerce law requires the operator's name on it,
# which makes it the highest-yield source of a named human for this list --
# higher than LinkedIn, since a small shop that has no LinkedIn presence at all
# still has to publish this.
LEGAL_PAGE_RE = re.compile(
    r"""href\s*=\s*["']([^"']*(?:impressum|imprint|legal-?notice|aviso-?legal"""
    r"""|mentions-?legales|note-?legali|informazioni-?legali|colofon"""
    r"""|informacion-?legal|legal-?information|kontakt-?impressum)[^"']*)["']""", re.I)

# Labels that introduce the responsible person on a legal-notice page, by
# locale. The name follows the label.
# Labels that introduce the responsible person on a legal-notice page, by
# locale. The name follows the label, at most one line break away -- word
# separators are spaces rather than \s, or the pattern runs past the end of
# the line and swallows the next label as a surname.
_NAME_WORD = (r"[A-ZÄÖÜÁÉÍÓÚÀÈÌÒÙÂÊÎÔÛÇÑ]"
              r"[\wÄÖÜäöüßÁÉÍÓÚáéíóúÀÈÌÒÙàèìòùÂÊÎÔÛâêîôûÇçÑñ'’\-]{1,30}")
# Lowercase particles carried by Dutch, German, Spanish, Italian and French
# surnames -- "Jan de Vries", "Anna van der Berg". Requiring every word to be
# capitalised drops these names entirely.
_NAME_PARTICLE = (r"(?:de|del|della|di|da|dos|du|van|von|der|den|ter|te|le|la"
                  r"|el|al|bin|ibn|of|op|het|'t)")

OWNER_LABELS = re.compile(
    r"""(?:Vertreten\s+durch|Inhaber(?:in)?|Gesch[aä]ftsf[uü]hrer(?:in)?"""
    r"""|Vertretungsberechtigt(?:er|e)?|Eigent[uü]mer(?:in)?"""
    r"""|Titular|Responsable|Representante\s+legal"""
    r"""|Legale\s+rappresentante|Rappresentante\s+legale|Titolare"""
    r"""|Directeur\s+de\s+la\s+publication|Responsable\s+de\s+publication"""
    r"""|Eigenaar|Bestuurder"""
    r"""|Owner|Managing\s+Director|Proprietor)"""
    r"""[ \t]*[:\-–]?[ \t]*\n?[ \t]*"""
    # Honorifics, then any run of dotted abbreviations -- "Dipl.-Ing.",
    # "Dr. med." -- consumed before the name so the capture can start on a
    # letter rather than failing on the dot.
    r"""(?:(?:Herr|Frau|Mr|Mrs|Ms|Dhr|Mevr)\.?[ \t]*)*"""
    r"""(?:[A-Za-zÄÖÜäöüß]{2,24}\.[ \t]*-?[ \t]*)*"""
    + f"({_NAME_WORD}(?:[ \\t]+{_NAME_PARTICLE})*(?:[ \\t]+{_NAME_WORD}){{1,3}})")

# Honorifics and job titles that sit in front of the name on a legal notice.
# "Ministerialdirektor Hubert Bittlmayer" is a title plus a name, not a
# three-part name.
OWNER_TITLES = {
    "dr", "prof", "dipl", "ing", "mag", "mba", "msc", "bsc", "med", "phd",
    "herr", "frau", "mr", "mrs", "ms", "dhr", "mevr", "sr", "sra", "don",
    "ministerialdirektor", "ministerialrat", "direktor", "director",
    "geschäftsführer", "geschaeftsfuehrer", "inhaber", "inhaberin",
    "eigentümer", "eigentuemer", "vorstand", "president", "presidente",
    "rechtsanwalt", "steuerberater", "apotheker", "architekt",
}

# Words that look like a name to the pattern above but are a company form.
OWNER_STOPWORDS = {
    "gmbh", "ug", "kg", "ohg", "gbr", "ag", "mbh", "co", "kgaa", "ev", "e.v",
    "ltd", "limited", "llc", "inc", "plc", "bv", "b.v", "nv", "n.v", "sarl",
    "sas", "sa", "srl", "spa", "s.r.l", "sl", "s.l", "sp", "oy", "ab", "aps",
    "gmbh&co", "unternehmergesellschaft", "haftungsbeschränkt", "company",
    "the", "our", "this", "die", "der", "das", "und", "siehe", "oben",
}

IMG_TAG_RE = re.compile(r"<img\b[^>]*>", re.I)
ATTR_RE = re.compile(r"""([a-zA-Z_:][-\w:.]*)\s*=\s*(?:"([^"]*)"|'([^']*)'|([^\s">]+))""")
OG_IMAGE_RE = re.compile(
    r"""<meta[^>]+(?:property|name)\s*=\s*["']og:image["'][^>]*>""", re.I)
META_DESC_RE = re.compile(
    r"""<meta[^>]+name\s*=\s*["']description["'][^>]+content\s*=\s*["']([^"']{1,400})["']""", re.I)

MODERN_IMAGE_EXT = (".webp", ".avif")
RASTER_IMAGE_EXT = (".jpg", ".jpeg", ".png", ".gif", ".bmp", ".tif", ".tiff")
