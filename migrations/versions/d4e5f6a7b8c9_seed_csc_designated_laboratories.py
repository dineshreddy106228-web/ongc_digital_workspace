"""Seed document-designated laboratories against the corporate register.

The controlled source is ``Designated Laboratories_Oil Field Chemicals.docx``.
This migration retains the 284 assignable rows from that document as compact,
lossless data, so the same designations reach local and Railway databases.
Four source rows are deliberately not seeded: two state "Not Being Used" and
two require vendor-premises testing/witnessing rather than naming a designated
laboratory. Existing manually recorded authorisations are never removed.

Revision ID: d4e5f6a7b8c9
Revises: c3d4e5f6a7b8
"""
from __future__ import annotations

import base64
import json
import re
import zlib
from collections import defaultdict
from datetime import datetime

import sqlalchemy as sa
from alembic import op

revision = "d4e5f6a7b8c9"
down_revision = "c3d4e5f6a7b8"
branch_labels = None
depends_on = None


# zlib + base85 of the 284 document rows as
# [specification number, API-grade chemical name when it has no spec number,
#  [designated lab codes]]. The decoded payload is ordinary UTF-8 JSON; compact
# storage keeps this data migration reviewable without a 19 KB raw literal.
_DOCUMENT_ASSIGNMENTS_B85 = (
    "c-p;NU2oeq6#XlKU)llzB7RekiHD{ElFeAU_8|xiwif1ej#J1^gYMVQQqwNf7s(@1j}~~&B`*(oFE8nvo7o?~pU+QzdpkeL$O+A;oE^_*$2YU`@6Gb7*?ucF"
    "hY!_{Z)WwdGNpO``Q@QpoBi{T(tI)N`^W97{64$AJ$^~b{74%p-bB5yez$rmmaAf2n$_dB2~<(W;4qR;V1#6YMt-pla*!!HDyJ}_dyNdis2J}=+LDwPj0O3="
    "e*rNzfqU5$%Z5*(MotrKVtk4@-~!z9xI}^0qOw~KZqGaifubb=+dtl`OVC(EB7t6nM1r1Bs8{m=0let8jgr{}qLx#vdC8|Rq8v{Cye_s|bJv7v3<gc9q7yhm"
    "F;J#zifK~VdLE=yeskg46_k#|llvt#VUE+(HLd+U(rkR1r8$)0=@k37mRMgD2E%e&?F_=tG&U^HAZt0nU=)Kf42Eea7Z?mvNv_8)mOPJeILu<4Fdan$PZAb`"
    "1e<)43Fv%B9Hj=&t%hX7au`A;i37wgJb3Rb#4a`j<OPI`2t9=KY3KiT^ZBW(4)rq&3-MM>%e`mMK_t`kPo<&NX=w8#w2o35OdB2&YZyffLk?b+h*w3DIMK0)"
    "a>(&C=)jArf)dQiASa!q=WG%yd=e|dbIA{q$Vh^v3KOiJ#8u(vhziCsXiN;-G#Twp(!xgDE~leJe7FKRh!v59HKJ~h$mQewZR3{EKUu4>!Iyy_ug6?V_K(rF"
    "K#^{#<lWNAE##>!5GZol6fAisd2$QM#X^#crJdM#-C<Tp>Oi&vOa<aewzj9;Zh81wtjs5~I{Y@<e>TN#{bb6e+3kw`a$`OnK0lUbyZpK_+aDk7o{8kHmdPzB"
    "WJ)6CBNk~pAp6RSA@g7}B=<8<9=t|ec>7vNzCCg|{;fnsgNT}I1Qbwu>kMk5l|9?8>M|;b*O|t~j8})v$0GA_boC1X%$ty~IW;_C-ASTA#MOH-Z@20X$96mF"
    "*^YU(;}I3G0AC&23^ZU;d)&V8f2lYml*Qat5%#7Lz_mby8&deM#OFn>W24pS5WFi_YT(bt;dRgH+-{YJuBt`Xd&*uP5;Ek4`(nK_ThliLLe55jgHNIri1|2;"
    "aKvd60V#h^Yk<F9$Mpemz_U~!mJpv&75N!M#i_mLf#%*-F$?D^g7poTiTJ~1G6AOo=91;{JHoTEQIe02vyPKSf8QmDQ&6(Dn7ea&Pp9W-2imE+pGAS}|AN8Y"
    "<S-K183nR43Kb=v5=f*GNTMN;@FYRONQxL^-hn|PVN)XFkcc>>f2tw1^HjAMe0Tfe6m&6<aVyavq_GLZFIz4`k1rk~tw}`+QVER@3o12K<Io%kffbNs$RJ5<"
    "agx~LBrG;z@bRWb3CO*W@_cV-Y!rnA;CO`6uQy6zjZ+4Muo}p5n<BI%dhzpsCIUg8hpD=4?%DP#u;}Dk2Uo7`yAwlVR`mXQf9V}V8mQo`p+-r_Rz5zdtawP4"
    "^lGFw5nA=HK{UzbY2#M(c(ykG-j$~FqO|TtY-JIFI71@BA__w3^n%C%AH;h5*kGl}N9%Z$XzQ~j$Kir&&Cka@P^mctvIj-D<Zlp0_@7$M<MjuX1}6CbiWQ1w"
    "$Qok=LIt0cFBj+D$GirLLnsNMLY`;4H*n`IO>I*{zU<$)TK(!*XvGwMDymUFH=%XI2v>MDkAS-AxM^-z*!zeyWIjhw6q*s|ld|?kSHmb|j6o=9DccdDQ{A0Q"
    "ghE?D*yT}=Pa&~TNGvR5{th3{O!<9Z9GxyMj?PQ7DtyFJ!HVtvVSDF5cD-P3GR?2%N3Y*~c>g92(`5{%cc!>E`_9n~{=4Du^?Gm0&ubg!)%ndKARN6qe|0o3"
    "?hnpA_~iP}Hz7Q?{{eSWC}{"
)


def document_assignments() -> list[tuple[str, str, tuple[str, ...]]]:
    """Return the exact assignable rows extracted from the controlled source."""
    decoded = zlib.decompress(base64.b85decode(_DOCUMENT_ASSIGNMENTS_B85)).decode("utf-8")
    return [(spec, chemical, tuple(codes)) for spec, chemical, codes in json.loads(decoded)]


def _normal(value: str | None) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(value or "").casefold())


def _spec_without_year(value: str | None) -> str:
    return re.sub(r"\d{4}$", "", _normal(value))


def upgrade():
    bind = op.get_bind()
    metadata = sa.MetaData()
    standards = sa.Table("qc_testing_standards", metadata, autoload_with=bind)
    authorised_labs = sa.Table("csc_authorized_labs", metadata, autoload_with=bind)

    standard_rows = bind.execute(
        sa.select(standards.c.id, standards.c.specification_no, standards.c.chemical_name)
    ).mappings().all()
    by_spec = defaultdict(list)
    by_spec_without_year = defaultdict(list)
    by_name = defaultdict(list)
    for row in standard_rows:
        if _normal(row["specification_no"]):
            by_spec[_normal(row["specification_no"])].append(row["id"])
            by_spec_without_year[_spec_without_year(row["specification_no"])].append(row["id"])
        if _normal(row["chemical_name"]):
            by_name[_normal(row["chemical_name"])].append(row["id"])

    held = {
        (row["entry_ref"], row["lab_code"])
        for row in bind.execute(
            sa.select(authorised_labs.c.entry_ref, authorised_labs.c.lab_code)
        ).mappings()
    }
    inserts = []
    now = datetime.utcnow()
    for specification_no, chemical_name, lab_codes in document_assignments():
        if specification_no:
            record_ids = by_spec.get(_normal(specification_no), ())
            if not record_ids:
                # Some local rows retain the controlled pre-2026 revision. Apply
                # the designation to every same-series row rather than choose a
                # similarly named but unrelated chemical.
                record_ids = by_spec_without_year.get(_spec_without_year(specification_no), ())
        else:
            record_ids = by_name.get(_normal(chemical_name), ())
        for record_id in record_ids:
            entry_ref = f"r-{record_id}"
            for lab_code in lab_codes:
                key = (entry_ref, lab_code)
                if key in held:
                    continue
                inserts.append({"entry_ref": entry_ref, "lab_code": lab_code, "updated_at": now})
                held.add(key)
    if inserts:
        bind.execute(authorised_labs.insert(), inserts)


def downgrade():
    # Do not delete data on downgrade: the table is itself removed by the prior
    # schema migration, and deleting here could erase a later manual edit.
    pass
