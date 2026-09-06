"""
Corrected ISO 20022 camt.054 normalizer.

FIX vs original draft: the original used `ntfctn.find('.//ns:Ntry', ns)`, which
returns only the FIRST <Ntry> element. Verified against a two-entry sample
notification: the original silently dropped the second entry with no error,
no log, and no exception. Real bank gateways commonly batch multiple entries
into one <Ntfctn>. This version parses ALL entries and returns a list, so the
caller processes every settled transaction in the notification rather than
just the first.
"""
from decimal import Decimal
from datetime import datetime
from typing import Optional, List
from pydantic import BaseModel, Field
import defusedxml.ElementTree as ET


class NormalizedTransaction(BaseModel):
    tenant_id: str
    message_id: str
    utr: str
    van: str
    amount: Decimal = Field(..., max_digits=18, decimal_places=4)
    currency: str = Field(default="INR", max_length=3)
    event_timestamp: datetime
    debtor_name: Optional[str] = None
    raw_format: str


class PaymentIngestionNormalizer:
    NS = {"ns": "urn:iso:std:iso:20022:tech:xsd:camt.054.001.08"}

    @classmethod
    def parse_iso20022_camt054(cls, xml_content: str, tenant_id: str) -> List[NormalizedTransaction]:
        """
        Parses a camt.054.001.08 notification and returns ONE NormalizedTransaction
        per <Ntry> element (a notification may legitimately contain several).
        Raises ValueError with a specific message on missing required fields,
        rather than an unhandled AttributeError from a None .find() result.
        """
        root = ET.fromstring(xml_content)
        ns = cls.NS

        grp_hdr = root.find(".//ns:GrpHdr", ns)
        if grp_hdr is None or grp_hdr.find("ns:MsgId", ns) is None:
            raise ValueError("Missing required GrpHdr/MsgId")
        msg_id = grp_hdr.find("ns:MsgId", ns).text

        ntfctn = root.find(".//ns:Ntfctn", ns)
        if ntfctn is None:
            raise ValueError("Missing required Ntfctn element")

        van_elem = ntfctn.find(".//ns:Acct/ns:Id/ns:Othr/ns:Id", ns)
        if van_elem is None:
            raise ValueError("Missing required Acct/Id/Othr/Id (VAN)")
        van = van_elem.text

        entries = ntfctn.findall(".//ns:Ntry", ns)   # FIX: findall, not find — capture every entry
        if not entries:
            raise ValueError("Notification contains no Ntry elements")

        results: List[NormalizedTransaction] = []
        for idx, ntry in enumerate(entries):
            amt_elem = ntry.find("./ns:Amt", ns)
            if amt_elem is None or amt_elem.text is None:
                raise ValueError(f"Ntry[{idx}] missing Amt")
            amount = Decimal(amt_elem.text)
            currency = amt_elem.attrib.get("Ccy", "INR")

            bookg_dt = ntry.find(".//ns:BookgDt/ns:DtTm", ns)
            if bookg_dt is None or bookg_dt.text is None:
                raise ValueError(f"Ntry[{idx}] missing BookgDt/DtTm")
            event_time = datetime.fromisoformat(bookg_dt.text.replace("Z", "+00:00"))

            tx_dtls = ntry.find(".//ns:NtryDtls/ns:TxDtls", ns)
            if tx_dtls is None:
                raise ValueError(f"Ntry[{idx}] missing NtryDtls/TxDtls")
            refs = tx_dtls.find(".//ns:Refs/ns:TxId", ns)
            if refs is None or refs.text is None:
                raise ValueError(f"Ntry[{idx}] missing Refs/TxId (UTR)")
            utr = refs.text

            debtor_elem = tx_dtls.find(".//ns:RltdPties/ns:Dbtr/ns:Nm", ns)
            debtor_name = debtor_elem.text if debtor_elem is not None else None

            results.append(
                NormalizedTransaction(
                    tenant_id=tenant_id,
                    message_id=msg_id,
                    utr=utr,
                    van=van,
                    amount=amount,
                    currency=currency,
                    event_timestamp=event_time,
                    debtor_name=debtor_name,
                    raw_format="ISO_20022_CAMT054",
                )
            )
        return results
