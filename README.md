# DiAna
Repository for FAERS cleaning and analysis

# Download the ZIP with the FAERS dataset cleaned according to the scripts here documented: 
https://osf.io/zqu89/files/osfstorage

To cite DiAna in publication use:
Fusaroli M, Giunchi V. DiAna: Repository for FAERS cleaning and analysis.
Published online May 10, 2023. doi:10.17605/OSF.IO/ZQU89



DiAna (Disproportionality Analyses) is a project to develop a shared procedure to clean and analyse the FDA Adverse Event Reporting System database for signal detection.

FDA makes available different downloadable dataframes (xml, ascii), that differ partly from each other and from the public dashboard. Furthermore, these databases have duplicates and are not entirely standardized. Authors are required to perform a cleaning procedure before conducting the analyses, and different choices in this step can result in even more different and less comparable dataframes, with the result that two studies on the same subject using the FAERS database may retrieve a different number of cases and may detect different signals. Many private platforms are keeping their cleaning procedure secret and make available, for a price, a support service for signal detection. Other databases have been made public and available to everyone but are not updated anymore, or their pre-processing is not transparent and it is proposed as made of objective choice. Further, many public dashboards allow some degree of descriptive and disproportionality analyses, but they are often non-flexible and don’t allow to design and implement specific strategies to correct for the many biases that affect pharmacovigilance. Also, some packages have been developed to increase the feasibility, but these are often poorly documented and offer the possibility to only delve the surface of signal detection.

In this project, we discuss the different choices that have to be done when cleaning the FAERS database. We design a shared and transparent cleaning procedure using the R software. We share it together with the cleaned database, with the idea that an open source, clearly-described procedure may build the basis for a global common effort towards a shared cleaning procedure.

The next step will be to build a connected package for standardized analyses on the cleaned database, with the idea that also this tool may be implemented with time through a common effort to allow everyone more and more complex and in-deep analyses of post-marketing drug safety. A common package could allow also to share innovative pharmacovigilance tools.


Stages

[X] Mapping of existing opportunities to access the FAERS

[X] DiAna dictionary for drug name standardization (continuous refinement)

[X] DiAna access to a clean FAERS with all quarters collapsed together (continuous refinement). 

[ ] Probabilistic deduplication (work in progress)

[ ] Imputation of missing data

[X] DiAna R package for drug safety investigations

[ ] Linkage with other ontologies


System and Software
The script was tested on the following operative systems:

Michele’s Macbook

- **Platform**: aarch64-apple-darwin20
- **RAM**: 8 GB
- **Processor**: Apple M1
- **Language**: R version 4.3.1 (2023-06-16)
- **Software**: RStudio Version 2023.09.0+463 "Desert Sunflower"

Valentina’s Windows

- **Platform**: Windows 11 Pro version 22H2
- **RAM**: 16 GB
- **Processor**: 11th Gen Intel(R) Core(TM) i7-1165G7 @ 2.80GHz   1.69 GHz
- **Language**: R version 4.3.1 (2023-06-16 ucrt) -- "Beagle Scouts"
- **Software**: RStudio Version 2023.03.1 Build 446



