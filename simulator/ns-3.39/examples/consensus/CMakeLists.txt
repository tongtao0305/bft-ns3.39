build_example(
  NAME bidl-experiment
  SOURCE_FILES bidl-experiment.cc
  LIBRARIES_TO_LINK
    ${libconsensus}
    ${libcsma}
    ${libpoint-to-point}
    ${libinternet}
)
