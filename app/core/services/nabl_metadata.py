"""Static metadata for the NABL AccredKit module."""

from __future__ import annotations


LAB_ROLE_OPTIONS = [
    "Laboratory Head",
    "Quality Manager",
    "Senior Chemist",
    "Chemist",
    "Laboratory Technician",
]

STORAGE_OPTIONS = [
    "Physical + Electronic",
    "Electronic",
    "Physical",
]

DEFAULT_RECORD_RESPONSIBLE = "Quality Manager"
DEFAULT_RECORD_STORAGE = "Physical + Electronic"

LABS = {
    "idwe": {
        "lab_id": "idwe",
        "display_name": "Institute of Drilling and Well Engineering (IDWE)",
        "location": "Dehradun",
        "parent_organisation": "Oil and Natural Gas Corporation Limited (ONGC)",
        "approved_by": "Laboratory Head, IDWE",
        "lab_code": "IDWE",
        "hints": ["idwe", "drilling", "well engineering", "dehradun"],
    },
    "rgl_panvel": {
        "lab_id": "rgl_panvel",
        "display_name": "Regional Geosciences Laboratory, Panvel",
        "location": "Panvel, Navi Mumbai",
        "parent_organisation": "Oil and Natural Gas Corporation Limited (ONGC)",
        "approved_by": "Regional Manager, RGL Panvel",
        "lab_code": "PANVEL",
        "hints": ["panvel", "navi mumbai", "rgl panvel"],
    },
    "rgl_vadodara": {
        "lab_id": "rgl_vadodara",
        "display_name": "Regional Geosciences Laboratory, Vadodara",
        "location": "Vadodara",
        "parent_organisation": "Oil and Natural Gas Corporation Limited (ONGC)",
        "approved_by": "Regional Manager, RGL Vadodara",
        "lab_code": "VADODARA",
        "hints": ["vadodara", "baroda", "rgl vadodara"],
    },
    "rgl_jorhat": {
        "lab_id": "rgl_jorhat",
        "display_name": "Regional Geosciences Laboratory, Jorhat",
        "location": "Jorhat, Assam",
        "parent_organisation": "Oil and Natural Gas Corporation Limited (ONGC)",
        "approved_by": "Regional Manager, RGL Jorhat",
        "lab_code": "JORHAT",
        "hints": ["jorhat", "assam", "rgl jorhat"],
    },
    "rgl_rajahmundry": {
        "lab_id": "rgl_rajahmundry",
        "display_name": "Regional Geosciences Laboratory, Rajahmundry",
        "location": "Rajahmundry",
        "parent_organisation": "Oil and Natural Gas Corporation Limited (ONGC)",
        "approved_by": "Regional Manager, RGL Rajahmundry",
        "lab_code": "RAJAHMUNDRY",
        "hints": ["rajahmundry", "rgl rajahmundry"],
    },
    "rgl_chennai": {
        "lab_id": "rgl_chennai",
        "display_name": "Regional Geosciences Laboratory, Chennai",
        "location": "Chennai",
        "parent_organisation": "Oil and Natural Gas Corporation Limited (ONGC)",
        "approved_by": "Regional Manager, RGL Chennai",
        "lab_code": "CHENNAI",
        "hints": ["chennai", "madras", "rgl chennai"],
    },
}

DEFAULT_CHEMICALS = {
    "idwe": [
        ("Bentonite", "ONGC/MC/04/2015"),
        ("XC Polymer", "ONGC/MC/73/2015"),
        ("PAC LVG", "ONGC/MC/48/2015"),
        ("PAC RG", "ONGC/MC/49/2015"),
        ("Pregelatinized Starch", "ONGC/MC/57/2015"),
    ],
    "rgl_panvel": [
        ("Acid Corrosion Inhibitor (Grade II)", "ONGC/MC/xx/xxxx"),
        ("Corrosion Inhibitor (H2S)", "ONGC/MC/xx/xxxx"),
        ("Demulsifier for Mumbai Assets", "ONGC/MC/xx/xxxx"),
        ("Low Temperature Demulsifier (Heera)", "ONGC/MC/xx/xxxx"),
        ("Flow Improver (Ratna R-Series)", "ONGC/MC/xx/xxxx"),
        ("Causticised Lignite", "ONGC/MC/xx/xxxx"),
        ("Silica Fume", "ONGC/MC/xx/xxxx"),
        ("Bactericide (Amine Type)", "ONGC/MC/xx/xxxx"),
        ("Bactericide (Non-Aldehyde Non-Amine)", "ONGC/MC/xx/xxxx"),
        ("Bactericide (Aldehyde Type)", "ONGC/MC/xx/xxxx"),
    ],
    "rgl_vadodara": [
        ("Gelling Agents", "ONGC/MC/xx/xxxx"),
        ("Corrosion Inhibitor for Oil Line", "ONGC/MC/xx/xxxx"),
        ("Demulsifier (Padra GGS)", "ONGC/MC/xx/xxxx"),
        ("PHPA for Water Shut Off", "ONGC/MC/xx/xxxx"),
        ("PPD/Flow Improver (Kathana GGS)", "ONGC/MC/xx/xxxx"),
        ("PPD/Flow Improver (Anklav Field)", "ONGC/MC/xx/xxxx"),
    ],
    "rgl_jorhat": [
        ("EP Lubricant", "ONGC/MC/xx/xxxx"),
        ("Water Soluble Demulsifier (Lakwa)", "ONGC/MC/xx/xxxx"),
        ("Pour Point Depressant (Assam)", "ONGC/MC/xx/xxxx"),
        ("Oil Soluble Demulsifier (Assam)", "ONGC/MC/xx/xxxx"),
        ("High Performance EP Lubricant", "ONGC/MC/xx/xxxx"),
    ],
    "rgl_rajahmundry": [
        ("Surfactant", "ONGC/MC/xx/xxxx"),
        ("Demulsifier (Rajahmundry)", "ONGC/MC/xx/xxxx"),
        ("Deoiler (Rajahmundry)", "ONGC/MC/xx/xxxx"),
        ("PPD (Gopavaram Crude)", "ONGC/MC/xx/xxxx"),
        ("Bactericide (Aldehyde Type)", "ONGC/MC/xx/xxxx"),
        ("Bactericide (Amine Type)", "ONGC/MC/xx/xxxx"),
    ],
    "rgl_chennai": [
        ("Polyol Grade-I", "ONGC/MC/xx/xxxx"),
        ("Polyol Grade-II", "ONGC/MC/xx/xxxx"),
        ("Demulsifier (Cauvery Asset)", "ONGC/MC/xx/xxxx"),
        ("Xylol (Industrial Xylene)", "ONGC/MC/xx/xxxx"),
        ("Toluole (Industrial Toluene)", "ONGC/MC/xx/xxxx"),
        ("Baryte API 4.1", "ONGC/MC/xx/xxxx"),
    ],
}

STANDARD_ANSWERS = {
    "Q1.2": (
        "Testing is performed as per the parameters specified in the respective "
        "ONGC Material Specification. All test parameters listed in the "
        "applicable specification are covered in the scope of accreditation."
    ),
    "Q1.3": (
        "Units and measurement ranges are as specified in the applicable ONGC "
        "Material Specification for each chemical. These are documented in the "
        "Scope of Accreditation Register (REC-04B)."
    ),
    "Q1.4": (
        "All testing is performed in accordance with the applicable ONGC Material "
        "Specifications (ONGC/MC series) and any referenced IS, ASTM, API, or "
        "BIS standards cited therein. Internal controlled copies are maintained "
        "as SOP-TP series procedures."
    ),
    "Q2A.2": (
        "The QA Laboratory operates as a technically independent unit within "
        "[Lab Name]. While administratively part of ONGC, the Laboratory Head has "
        "sole authority over all technical decisions, test results and reports. "
        "No commercial, procurement or management pressure influences laboratory "
        "activities. This is documented in the Impartiality Policy (SOP-QS-01) "
        "and affirmed annually through the Impartiality Risk Register (REC-01B)."
    ),
    "Q2A.3": (
        "The laboratory operates with four defined roles: Laboratory Head "
        "(Chief Manager / equivalent), Senior Chemist / Quality Manager, "
        "Chemist, and Laboratory Technician. The Laboratory Head reports to the "
        "Director / Head of [Lab Name]. The organisational structure is "
        "documented in REC-04A."
    ),
    "Q2B.1": (
        "Personnel performing tests hold a minimum qualification of B.Sc. "
        "(Chemistry) or equivalent. Senior roles require M.Sc. (Chemistry) or "
        "B.Tech (Chemical Engineering) with relevant experience. All "
        "qualifications are verified at appointment and recorded in personnel "
        "files."
    ),
    "Q2B.2": (
        "Competence is assessed through a structured process: review of "
        "educational qualifications, supervised practice on a minimum of five "
        "test runs, a witnessed test assessed by the Senior Chemist, and "
        "written authorisation by the Laboratory Head. Assessment is recorded "
        "in REC-03D. Authorisation is recorded in REC-03B."
    ),
    "Q2B.3": (
        "Training needs are identified annually through the Training Needs "
        "Analysis documented in REC-03A. Training is delivered through "
        "on-the-job instruction, internal demonstrations, and external courses "
        "where required. All training is recorded in REC-03C. Effectiveness is "
        "assessed before authorisation is granted."
    ),
    "Q2B.4": (
        "Ongoing competence is monitored through annual witnessed tests, review "
        "of QC performance data, and participation in proficiency testing "
        "schemes. Any personnel whose competence is in doubt is suspended from "
        "testing until reassessed. Supervision records are maintained in REC-03E."
    ),
    "Q2C.1": (
        "The laboratory is a dedicated, controlled-access facility located "
        "within [Lab Name], [Location]. It is equipped with a testing area, "
        "sample storage area, chemical storage area, and a data recording "
        "station. Access is restricted to authorised laboratory personnel."
    ),
    "Q2C.2": (
        "Temperature and relative humidity are monitored continuously using "
        "calibrated instruments. Acceptable ranges are: Temperature 18–28°C, "
        "Relative Humidity 30–70%. Readings are recorded twice daily in "
        "REC-07A. Testing is suspended if conditions fall outside acceptable "
        "limits."
    ),
    "Q2C.3": (
        "The laboratory is cleaned daily per a defined schedule. Maintenance "
        "activities are recorded in REC-07C. Distilled water quality is checked "
        "daily and recorded in REC-07D. Any facility defect affecting test "
        "quality is reported to the Laboratory Head immediately."
    ),
    "Q2C.4": (
        "Access is restricted to authorised laboratory personnel. Visitors "
        "including NABL assessors, service engineers and ONGC audit teams must "
        "sign a Confidentiality Agreement (REC-02A) and are escorted at all "
        "times. All visitors are logged in REC-07B."
    ),
    "Q2D.2": (
        "Each instrument is assigned a unique Equipment ID in the format "
        "[LAB]-EQ-[NNN] (e.g. IDWE-EQ-001). The ID is affixed to the instrument "
        "as a label. All equipment is listed in the Equipment Master Register "
        "(REC-09A)."
    ),
    "Q2D.3": (
        "Calibration is performed by NABL-accredited external calibration "
        "laboratories. The calibration schedule is maintained in REC-09B. "
        "Individual calibration records and certificates are filed in REC-09C. "
        "Equipment is labelled with calibration status and due date. "
        "Out-of-calibration equipment is immediately taken out of service and "
        "recorded in REC-09E."
    ),
    "Q2D.4": (
        "Certified Reference Materials are used for method verification and QC "
        "purposes where applicable. CRMs are sourced from recognised suppliers "
        "with valid certificates of analysis. CRM receipt, use and disposal are "
        "recorded in REC-12B."
    ),
    "Q2D.5": (
        "Equipment that is found defective, out of calibration, or otherwise "
        "unfit for use is immediately labelled OUT OF SERVICE and removed from "
        "the testing area. The defect is recorded in REC-09E. The Laboratory "
        "Head determines whether affected test results need to be reviewed. "
        "Equipment is returned to service only after repair and verification."
    ),
    "Q2E.1": (
        "Samples are received from ONGC procurement or field locations. Upon "
        "receipt, each sample is inspected for condition, assigned a Laboratory "
        "Sample Number (LSN) in the format [LAB]-[YEAR]-[NNN], and registered "
        "in the Sample Receipt Register (REC-22A). Condition at receipt is "
        "documented in REC-22B."
    ),
    "Q2E.2": (
        "The Laboratory Sample Number (LSN) is the unique identifier used "
        "throughout — on the sample container, test worksheet, and final report. "
        "The supplier identity is not written on the sample label presented to "
        "the analyst, ensuring impartial testing. Traceability from receipt to "
        "result is maintained through REC-22A, REC-23 series worksheets, and "
        "REC-28A test reports."
    ),
    "Q2E.3": (
        "Non-conforming samples (damaged, leaking, incorrectly labelled, or "
        "insufficient quantity) are segregated and labelled with an NC tag. "
        "The condition is recorded in REC-22B and the customer is notified. "
        "Testing proceeds only after customer instruction and with appropriate "
        "caveats noted in the report."
    ),
    "Q2E.4": (
        "Each test request is received on a Test Request Form (REC-17A). The "
        "laboratory checks that the requested tests are within scope, that the "
        "method is defined, and that the laboratory has the capacity to deliver. "
        "This review is documented in REC-17B. Any deviations or special "
        "requirements are communicated to the customer and recorded in REC-17C "
        "before work begins."
    ),
    "Q2F.1": (
        "Quality control samples (blanks, duplicates, or reference standards as "
        "applicable) are run with every batch of samples. A minimum of one QC "
        "check per ten test samples is performed. QC results are plotted on "
        "control charts (REC-27A) and reviewed by the Senior Chemist before "
        "results are reported."
    ),
    "Q2F.2": (
        "If a QC result is outside control limits, testing is suspended for that "
        "parameter. The cause is investigated and recorded in REC-27D. "
        "Affected samples are re-tested after the cause is resolved. If the "
        "issue cannot be resolved, the Laboratory Head is informed and a CAPA "
        "is raised (REC-36A)."
    ),
    "Q2F.3": (
        "The laboratory participates in NABL-approved Proficiency Testing (PT) "
        "schemes relevant to its scope of accreditation, at a minimum frequency "
        "of once per year per discipline. PT participation and results are "
        "recorded in REC-27B. Unsatisfactory PT results trigger a CAPA "
        "(REC-36A)."
    ),
    "Q2F.4": (
        "Measurement uncertainty is estimated for each test parameter using the "
        "repeatability and reproducibility approach based on in-house validation "
        "data. Uncertainty budgets are documented in REC-25A and supporting "
        "repeatability study data in REC-25B. Expanded uncertainty at 95% "
        "confidence level (k=2) is reported on test reports where required by "
        "the customer or NABL."
    ),
    "Q2G.1": (
        "Each test report contains: unique report number, date of issue, "
        "laboratory name and address, customer name, sample description and "
        "LSN, date of receipt, test method reference, test results with units, "
        "pass/fail statement against specification, measurement uncertainty "
        "(where applicable), authorised signatory name and designation, and a "
        "statement that results relate only to the sample tested."
    ),
    "Q2G.2": (
        "Test reports are prepared by the Chemist performing the test, "
        "independently reviewed by the Senior Chemist for technical correctness, "
        "and authorised by the Laboratory Head before issue. The review and "
        "authorisation are recorded in REC-28B. No report is issued without the "
        "Laboratory Head's signature."
    ),
    "Q2G.3": (
        "Amendments to issued reports are made only with the Laboratory Head's "
        "authorisation. The amended report is clearly identified as a revision "
        "with the original report number retained. The reason for amendment is "
        "recorded in REC-28C. The customer is notified of the amendment in "
        "writing."
    ),
    "Q2G.4": (
        "Standard turnaround time is five working days from sample receipt to "
        "report issue, subject to sample condition and test complexity. Any "
        "deviation from the committed turnaround is communicated to the customer "
        "promptly and recorded in REC-17C."
    ),
    "Q3": (
        "Based on your answers, your laboratory is required to maintain the "
        "following records as blank templates and as filled records during "
        "operation. All records listed below are pre-formatted for your "
        "laboratory. Review the responsible person, retention period and storage "
        "method for each, and confirm."
    ),
}

WIZARD_SECTIONS = [
    {"slug": "q1-scope", "code": "Q1.1", "step": "Q1", "title": "Chemical scope", "type": "chemical_table"},
    {"slug": "q1-parameters", "code": "Q1.2", "step": "Q1", "title": "Test parameters per chemical", "type": "choice_text"},
    {"slug": "q1-units", "code": "Q1.3", "step": "Q1", "title": "Measurement units and ranges", "type": "choice_text"},
    {"slug": "q1-methods", "code": "Q1.4", "step": "Q1", "title": "Test method standards", "type": "choice_text"},
    {"slug": "q2a-identity", "code": "Q2A.1", "step": "Q2", "title": "Lab identity and authority", "type": "lab_profile"},
    {"slug": "q2a-independence", "code": "Q2A.2", "step": "Q2", "title": "Organisational independence", "type": "choice_text"},
    {"slug": "q2a-roles", "code": "Q2A.3", "step": "Q2", "title": "Roles and reporting structure", "type": "choice_text"},
    {"slug": "q2a-approval", "code": "Q2A.4", "step": "Q2", "title": "Approval authority", "type": "approval_authority"},
    {"slug": "q2b-qualifications", "code": "Q2B.1", "step": "Q2", "title": "Minimum qualifications", "type": "choice_text"},
    {"slug": "q2b-competence", "code": "Q2B.2", "step": "Q2", "title": "Competence assessment", "type": "choice_text"},
    {"slug": "q2b-training", "code": "Q2B.3", "step": "Q2", "title": "Training", "type": "choice_text"},
    {"slug": "q2b-monitoring", "code": "Q2B.4", "step": "Q2", "title": "Ongoing competence monitoring", "type": "choice_text"},
    {"slug": "q2c-description", "code": "Q2C.1", "step": "Q2", "title": "Facility description", "type": "choice_text"},
    {"slug": "q2c-environment", "code": "Q2C.2", "step": "Q2", "title": "Environmental monitoring", "type": "choice_text"},
    {"slug": "q2c-maintenance", "code": "Q2C.3", "step": "Q2", "title": "Facility maintenance", "type": "choice_text"},
    {"slug": "q2c-access", "code": "Q2C.4", "step": "Q2", "title": "Access control", "type": "choice_text"},
    {"slug": "q2d-entry", "code": "Q2D.1", "step": "Q2", "title": "Equipment entry method", "type": "equipment_entry"},
    {"slug": "q2d-identification", "code": "Q2D.2", "step": "Q2", "title": "Equipment identification", "type": "choice_text"},
    {"slug": "q2d-calibration", "code": "Q2D.3", "step": "Q2", "title": "Calibration management", "type": "choice_text"},
    {"slug": "q2d-crm", "code": "Q2D.4", "step": "Q2", "title": "Reference materials", "type": "choice_text"},
    {"slug": "q2d-breakdown", "code": "Q2D.5", "step": "Q2", "title": "Equipment breakdown", "type": "choice_text"},
    {"slug": "q2e-receipt", "code": "Q2E.1", "step": "Q2", "title": "Sample receipt", "type": "choice_text"},
    {"slug": "q2e-traceability", "code": "Q2E.2", "step": "Q2", "title": "Sample identification and traceability", "type": "choice_text"},
    {"slug": "q2e-nonconforming", "code": "Q2E.3", "step": "Q2", "title": "Non-conforming samples", "type": "choice_text"},
    {"slug": "q2e-contract-review", "code": "Q2E.4", "step": "Q2", "title": "Test request and contract review", "type": "choice_text"},
    {"slug": "q2f-frequency", "code": "Q2F.1", "step": "Q2", "title": "QC frequency", "type": "choice_text"},
    {"slug": "q2f-out-of-control", "code": "Q2F.2", "step": "Q2", "title": "Out-of-control response", "type": "choice_text"},
    {"slug": "q2f-pt", "code": "Q2F.3", "step": "Q2", "title": "Proficiency testing", "type": "choice_text"},
    {"slug": "q2f-uncertainty", "code": "Q2F.4", "step": "Q2", "title": "Measurement uncertainty", "type": "choice_text"},
    {"slug": "q2g-report-content", "code": "Q2G.1", "step": "Q2", "title": "Test report content", "type": "choice_text"},
    {"slug": "q2g-authorisation", "code": "Q2G.2", "step": "Q2", "title": "Report review and authorisation", "type": "choice_text"},
    {"slug": "q2g-amendments", "code": "Q2G.3", "step": "Q2", "title": "Report amendments", "type": "choice_text"},
    {"slug": "q2g-turnaround", "code": "Q2G.4", "step": "Q2", "title": "Turnaround time", "type": "choice_text"},
    {"slug": "q3-records", "code": "Q3", "step": "Q3", "title": "Records confirmation", "type": "records_table"},
]

WIZARD_SECTION_MAP = {section["slug"]: section for section in WIZARD_SECTIONS}

RETENTION_BY_SERIES = {
    "REC-01": "5 years",
    "REC-02": "Duration of engagement + 3 years",
    "REC-03": "Duration of employment + 5 years",
    "REC-04": "Current version + 2 previous versions",
    "REC-07": "3 years",
    "REC-09": "Life of equipment + 5 years",
    "REC-12B": "5 years",
    "REC-17": "5 years",
    "REC-20A": "Life of method + 5 years",
    "REC-22": "5 years",
    "REC-23": "10 years",
    "REC-25": "Life of method + 5 years",
    "REC-27": "5 years",
    "REC-28": "10 years",
    "REC-29": "5 years",
    "REC-30": "5 years",
    "REC-31": "5 years",
    "REC-32": "Current + 2 previous versions",
    "REC-33": "5 years",
    "REC-34A": "5 years",
    "REC-35": "5 years",
    "REC-36": "5 years",
    "REC-37": "5 years",
    "REC-38": "5 years",
}

DOCUMENT_CATALOG = [
    {"doc_id": "SOP-STR-01", "title": "Structural Requirements & Organisation", "clause": "5", "kind": "SOP", "layer": "layer1", "compile": True},
    {"doc_id": "QM-01", "title": "Quality Manual", "clause": "5 / 8.1-8.2", "kind": "SOP", "layer": "layer1", "compile": True},
    {"doc_id": "SOP-QS-01", "title": "Impartiality Management", "clause": "4.1", "kind": "SOP", "layer": "layer1", "compile": True},
    {"doc_id": "SOP-QS-02", "title": "Confidentiality Management", "clause": "4.2", "kind": "SOP", "layer": "layer1", "compile": True},
    {"doc_id": "SOP-QS-03", "title": "Personnel Competence & Training", "clause": "6.2", "kind": "SOP", "layer": "layer1", "compile": True},
    {"doc_id": "SOP-QS-04", "title": "Facilities & Environmental Control", "clause": "6.3", "kind": "SOP", "layer": "layer1", "compile": True},
    {"doc_id": "SOP-QS-05", "title": "Equipment Management", "clause": "6.4", "kind": "SOP", "layer": "layer1", "compile": True},
    {"doc_id": "SOP-QS-06", "title": "Metrological Traceability", "clause": "6.5", "kind": "SOP", "layer": "layer1", "compile": True},
    {"doc_id": "SOP-QS-07", "title": "External Provider Management", "clause": "6.6", "kind": "SOP", "layer": "layer1", "compile": True},
    {"doc_id": "SOP-QS-08", "title": "Request, Tender & Contract Review", "clause": "7.1", "kind": "SOP", "layer": "layer1", "compile": True},
    {"doc_id": "SOP-QS-09", "title": "Method Selection, Verification & Validation", "clause": "7.2", "kind": "SOP", "layer": "layer1", "compile": True},
    {"doc_id": "SOP-QS-10", "title": "Sample Receipt, Identification & Handling", "clause": "7.3-7.4", "kind": "SOP", "layer": "layer1", "compile": True},
    {"doc_id": "SOP-QS-11", "title": "Technical Records Management", "clause": "7.5", "kind": "SOP", "layer": "layer1", "compile": True},
    {"doc_id": "SOP-QS-12", "title": "Measurement Uncertainty Estimation", "clause": "7.6", "kind": "SOP", "layer": "layer1", "compile": True},
    {"doc_id": "SOP-QS-13", "title": "Quality Control & Proficiency Testing", "clause": "7.7", "kind": "SOP", "layer": "layer1", "compile": True},
    {"doc_id": "SOP-QS-14", "title": "Test Report Preparation & Authorisation", "clause": "7.8", "kind": "SOP", "layer": "layer1", "compile": True},
    {"doc_id": "SOP-QS-15", "title": "Complaint Handling", "clause": "7.9", "kind": "SOP", "layer": "layer1", "compile": True},
    {"doc_id": "SOP-QS-16", "title": "Nonconforming Work Management", "clause": "7.10", "kind": "SOP", "layer": "layer1", "compile": True},
    {"doc_id": "SOP-QS-17", "title": "Data & Information Management", "clause": "7.11", "kind": "SOP", "layer": "layer1", "compile": True},
    {"doc_id": "SOP-QS-18", "title": "Document Control", "clause": "8.3", "kind": "SOP", "layer": "placeholder", "compile": True},
    {"doc_id": "SOP-QS-19", "title": "Records Control", "clause": "8.4", "kind": "SOP", "layer": "placeholder", "compile": True},
    {"doc_id": "SOP-QS-20", "title": "Risk & Opportunity Management", "clause": "8.5", "kind": "SOP", "layer": "placeholder", "compile": True},
    {"doc_id": "SOP-QS-21", "title": "Improvement Management", "clause": "8.6", "kind": "SOP", "layer": "placeholder", "compile": True},
    {"doc_id": "SOP-QS-22", "title": "Corrective Action (CAPA)", "clause": "8.7", "kind": "SOP", "layer": "placeholder", "compile": True},
    {"doc_id": "SOP-QS-23", "title": "Internal Audit", "clause": "8.8", "kind": "SOP", "layer": "placeholder", "compile": True},
    {"doc_id": "SOP-QS-24", "title": "Management Review", "clause": "8.9", "kind": "SOP", "layer": "placeholder", "compile": True},
    {"doc_id": "REC-01A", "title": "Personnel Impartiality Declaration", "clause": "4.1", "kind": "REC", "layer": "layer1", "compile": True},
    {"doc_id": "REC-01B", "title": "Impartiality Risk Register", "clause": "4.1", "kind": "REC", "layer": "layer1", "compile": True},
    {"doc_id": "REC-02A", "title": "Personnel Confidentiality Agreement", "clause": "4.2", "kind": "REC", "layer": "layer1", "compile": True},
    {"doc_id": "REC-03A", "title": "Competence Requirements Matrix", "clause": "6.2", "kind": "REC", "layer": "layer1", "compile": True},
    {"doc_id": "REC-03B", "title": "Personnel Authorization Register", "clause": "6.2", "kind": "REC", "layer": "layer1", "compile": True},
    {"doc_id": "REC-03C", "title": "Training Record", "clause": "6.2", "kind": "REC", "layer": "layer1", "compile": True},
    {"doc_id": "REC-03D", "title": "Competence Assessment Record", "clause": "6.2", "kind": "REC", "layer": "layer1", "compile": True},
    {"doc_id": "REC-03E", "title": "Supervision Record", "clause": "6.2", "kind": "REC", "layer": "layer1", "compile": True},
    {"doc_id": "REC-04A", "title": "Organizational Chart", "clause": "5", "kind": "REC", "layer": "layer1", "compile": True},
    {"doc_id": "REC-04B", "title": "Scope of Accreditation Register", "clause": "5.3", "kind": "REC", "layer": "layer3", "compile": False},
    {"doc_id": "REC-04C", "title": "Job Descriptions / Role Profiles", "clause": "5.5", "kind": "REC", "layer": "layer1", "compile": True},
    {"doc_id": "REC-07A", "title": "Daily Environmental Monitoring Log", "clause": "6.3", "kind": "REC", "layer": "layer1", "compile": True},
    {"doc_id": "REC-07B", "title": "Facility Access / Visitor Register", "clause": "6.3", "kind": "REC", "layer": "layer1", "compile": True},
    {"doc_id": "REC-07C", "title": "Facility Maintenance and Cleaning Log", "clause": "6.3", "kind": "REC", "layer": "layer1", "compile": True},
    {"doc_id": "REC-07D", "title": "Distilled Water Quality Log", "clause": "6.3", "kind": "REC", "layer": "layer1", "compile": True},
    {"doc_id": "REC-09A", "title": "Equipment Master Register", "clause": "6.4", "kind": "REC", "layer": "layer1", "compile": True},
    {"doc_id": "REC-09B", "title": "Equipment Calibration Schedule", "clause": "6.4", "kind": "REC", "layer": "layer1", "compile": True},
    {"doc_id": "REC-09C", "title": "Individual Equipment Calibration Record", "clause": "6.4", "kind": "REC", "layer": "layer1", "compile": True},
    {"doc_id": "REC-09E", "title": "Equipment Defect / Out-of-Service Record", "clause": "6.4", "kind": "REC", "layer": "layer1", "compile": True},
    {"doc_id": "REC-09F", "title": "Equipment Maintenance Log", "clause": "6.4", "kind": "REC", "layer": "layer1", "compile": True},
    {"doc_id": "REC-12B", "title": "Certified Reference Material (CRM) Log", "clause": "6.4", "kind": "REC", "layer": "layer1", "compile": True},
    {"doc_id": "REC-17A", "title": "Test Request Form / Work Order", "clause": "7.1", "kind": "REC", "layer": "layer1", "compile": True},
    {"doc_id": "REC-17B", "title": "Contract Review Checklist", "clause": "7.1", "kind": "REC", "layer": "layer1", "compile": True},
    {"doc_id": "REC-17C", "title": "Customer Communication Log", "clause": "7.1", "kind": "REC", "layer": "layer1", "compile": True},
    {"doc_id": "REC-20A", "title": "Method Validation Report", "clause": "7.2.2", "kind": "REC", "layer": "layer1", "compile": True},
    {"doc_id": "REC-22A", "title": "Sample Receipt Register", "clause": "7.4", "kind": "REC", "layer": "layer1", "compile": True},
    {"doc_id": "REC-22B", "title": "Sample Condition Assessment Form", "clause": "7.4", "kind": "REC", "layer": "layer1", "compile": True},
    {"doc_id": "REC-23X", "title": "Test Worksheet (per chemical)", "clause": "7.5", "kind": "REC", "layer": "layer2", "compile": False},
    {"doc_id": "REC-25A", "title": "Measurement Uncertainty Budget (per chemical)", "clause": "7.6", "kind": "REC", "layer": "layer2", "compile": False},
    {"doc_id": "REC-25B", "title": "Repeatability Study Data Record (per chemical)", "clause": "7.6", "kind": "REC", "layer": "layer2", "compile": False},
    {"doc_id": "REC-27A", "title": "QC Control Chart (per chemical)", "clause": "7.7", "kind": "REC", "layer": "layer2", "compile": False},
    {"doc_id": "REC-27B", "title": "PT Participation Record", "clause": "7.7", "kind": "REC", "layer": "layer1", "compile": True},
    {"doc_id": "REC-27C", "title": "Duplicate / Replicate Test Record", "clause": "7.7", "kind": "REC", "layer": "layer1", "compile": True},
    {"doc_id": "REC-27D", "title": "QC Out-of-Control Investigation Record", "clause": "7.7", "kind": "REC", "layer": "layer1", "compile": True},
    {"doc_id": "REC-28A", "title": "Test Report Template (per chemical)", "clause": "7.8", "kind": "REC", "layer": "layer2", "compile": False},
    {"doc_id": "REC-28B", "title": "Report Review and Authorisation Record", "clause": "7.8", "kind": "REC", "layer": "layer1", "compile": True},
    {"doc_id": "REC-28C", "title": "Report Amendment Register", "clause": "7.8.8", "kind": "REC", "layer": "layer1", "compile": True},
    {"doc_id": "REC-28D", "title": "Customer Communication Log", "clause": "7.8.7", "kind": "REC", "layer": "layer1", "compile": True},
    {"doc_id": "REC-29A", "title": "Complaint Register", "clause": "7.9", "kind": "REC", "layer": "layer1", "compile": True},
    {"doc_id": "REC-29B", "title": "Complaint Investigation Report", "clause": "7.9", "kind": "REC", "layer": "layer1", "compile": True},
    {"doc_id": "REC-30A", "title": "Nonconforming Work Report Form", "clause": "7.10", "kind": "REC", "layer": "layer1", "compile": True},
    {"doc_id": "REC-30B", "title": "NCW Register", "clause": "7.10", "kind": "REC", "layer": "layer1", "compile": True},
    {"doc_id": "REC-30C", "title": "Customer Notification of NCW", "clause": "7.10", "kind": "REC", "layer": "layer1", "compile": True},
    {"doc_id": "REC-31A", "title": "Spreadsheet / LIMS Validation Record", "clause": "7.11", "kind": "REC", "layer": "layer1", "compile": True},
    {"doc_id": "REC-31B", "title": "Data Backup Log", "clause": "7.11", "kind": "REC", "layer": "layer1", "compile": True},
    {"doc_id": "REC-31C", "title": "System Failure Log", "clause": "7.11", "kind": "REC", "layer": "layer1", "compile": True},
    {"doc_id": "REC-32A", "title": "Document Master List", "clause": "8.3", "kind": "REC", "layer": "layer1", "compile": True},
    {"doc_id": "REC-32B", "title": "Document Change Record", "clause": "8.3", "kind": "REC", "layer": "layer1", "compile": True},
    {"doc_id": "REC-32C", "title": "External Document Register", "clause": "8.3", "kind": "REC", "layer": "layer1", "compile": True},
    {"doc_id": "REC-33A", "title": "Records Master List / Retention Schedule", "clause": "8.4", "kind": "REC", "layer": "layer1", "compile": True},
    {"doc_id": "REC-33B", "title": "Records Disposal Register", "clause": "8.4", "kind": "REC", "layer": "layer1", "compile": True},
    {"doc_id": "REC-34A", "title": "Risk Register", "clause": "8.5", "kind": "REC", "layer": "layer1", "compile": True},
    {"doc_id": "REC-35A", "title": "Improvement Log", "clause": "8.6", "kind": "REC", "layer": "layer1", "compile": True},
    {"doc_id": "REC-35B", "title": "Customer Feedback Record", "clause": "8.6", "kind": "REC", "layer": "layer1", "compile": True},
    {"doc_id": "REC-36A", "title": "CAPA Report Form", "clause": "8.7", "kind": "REC", "layer": "layer1", "compile": True},
    {"doc_id": "REC-36B", "title": "CAPA Register", "clause": "8.7", "kind": "REC", "layer": "layer1", "compile": True},
    {"doc_id": "REC-36C", "title": "CAPA Effectiveness Verification Record", "clause": "8.7", "kind": "REC", "layer": "layer1", "compile": True},
    {"doc_id": "REC-37A", "title": "Annual Internal Audit Programme", "clause": "8.8", "kind": "REC", "layer": "layer1", "compile": True},
    {"doc_id": "REC-37B", "title": "Internal Audit Report", "clause": "8.8", "kind": "REC", "layer": "layer1", "compile": True},
    {"doc_id": "REC-37C", "title": "Audit Findings Register", "clause": "8.8", "kind": "REC", "layer": "layer1", "compile": True},
    {"doc_id": "REC-38A", "title": "Management Review Minutes", "clause": "8.9", "kind": "REC", "layer": "layer1", "compile": True},
    {"doc_id": "REC-38B", "title": "Management Review Action Tracker", "clause": "8.9", "kind": "REC", "layer": "layer1", "compile": True},
    {"doc_id": "SOP-TP", "title": "Technical Test Procedure series (per chemical)", "clause": "Layer 3", "kind": "SOP", "layer": "layer2", "compile": False},
]

DOCUMENT_MAP = {row["doc_id"]: row for row in DOCUMENT_CATALOG}
