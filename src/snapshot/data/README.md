# README

A2CPS: the Acute to Chronic Pain Signatures project (<https://a2cps.org>)

This NIH funded initiative is generating data on participants who
undergo surgery in order to study chronic pain.

This BIDS dataset is an interim product of the A2CPS project, and
includes pre-surgery magnetic resonance imaging (MRI) data on the
subset of individuals designated as "Release 1" (597 cases).

This is specifically Release 1.1; see below for a brief summary of
changes from Release 1.0. The subjects included are unchanged from 1.0
but processing and documentation have been updated.

## Release 1.x selection

The Release 1 subject set consists of all A2CPS participants with
surgeries completed by February 2023. Only pre-surgery scans and
measures are included.

## Background and policies

See <https://confluence.a2cps.org/display/WG/First+Data+Release> for a
summary of the first release and the procedure for accessing data and
creating publications based on it.

## Contact information

For additional information, please contact

- DIRC imaging group, <a2cps-dirc-report-imaging@a2cps.org>

## History

Data collection began in late 2020, and continued during the COVID-19
pandemic, though with fluctuations due to temporary restrictions at
imaging sites. The collection of the first 50 scan sessions was
completed in July 2021. MCC1 sites began data collection first, with
the exception of RU (added 2024). MCC2 sites were added in 2021 upon
completion of their setup.

## Overview

The various data collection components of A2CPS were intended to
capture possible features that could predict the development of
chronic pain. In the case of imaging, there are structural MRI,
diffusion MRI, and functional MRI scans; for the last, two resting
state scans bracket a pair of scans during which pain was administered
via an inflated leg cuff.

### BIDS information

### Subjects

Subjects were patients undergoing one of two surgery types: knee (TKA,

Total Knee Arthroplasty) or thoracic. The numeric part of an A2CPS ID records
MCC (first digit) and surgery type (second digit):

- 10000-14999: MCC1 TKA
- 15000-19999: MCC1 Thoracic
- 20000-24999: MCC2 Thoracic
- 25000-29999: MCC2 TKA

### Task organization

The two tasks used a subject-specific cuff pressure (CUFF1) and a
standard pressure (CUFF2). In some cases, administration of cuff
pressure was contraindicated entirely, or the standard pressure was
too high to be tolerated, so that one or both of the Cuff scans were
skipped. In cases where both were skipped, the imaging protocol
allowed REST2 to be skipped as well, though it was occasionally
collected (especially prior to this being formalized in the
protocol).

Tasks were not counter-balanced. Counterbalancing would be desirable
in some respects, but posed a standardization issue across sites and
would have added a potential failure point. In addition, which is more
painful may vary across patients in ways hard to predict. We thus kept
the runs in the same order, with pressure calibrated to pain-40 (P40)
first and a standard intensity (120 mm Hg) second.

Recording of patient pain ratings was interspersed between the fMRIs.

### Additional data acquired

The project includes collection of blood samples (for proteomics,
lipidomics, metabolomics, exRNA, and gene variant measures),
psychosocial questionnaires, quantitative sensory testing (QST) and
functional testing measures, and medical record (EHR) data, all of
which are released separately.

### Experimental location

A2CPS MRIs are collected at seven imaging sites organized in two consortia
("multi-site clinical centers", or MCCs):

MCC1: Chicago, Illinois

- UI: University of Illinois Chicago
- UC: University of Chicago
- NS: NorthShore University HealthSystem
- RU: Rush University

MCC2: Michigan

- UM: University of Michigan (2 scanners)
- WS: Wayne State University
- SH: Corewell Health (formerly Spectrum Health)/Michigan State University Grand Rapids

All sites except RU are present in the Release 1 subject set.

### Site specific variations in protocols

In general, the A2CPS project used the protocol developed by the ABCD
study, and some sites used ABCD MRI sequences directly. However, on
older scanners some adjustments were required, and in particular the
number of slices, exact voxel size, and TE in fMRI and the exact b-values
in DWI scans can vary. Please see the Release Notes for details.

### Further details

Further details on the procedures for cuff pain administration, recruitment criteria, and
other points can be found in the Method of Procedures manual (MOP).

### Changes in this release

This Release (1.1) is based on the Release 1 participants, who are fixed
for all 1.x releases.

Notable changes:

- Documentation improvements (Release Notes have been added)
- Improvements in BIDS
- Improvements in derivatives
See the Release Notes document for more details.
