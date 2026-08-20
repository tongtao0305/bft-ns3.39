build_lib_example(
  NAME consensus-example
  SOURCE_FILES consensus-example.cc
  LIBRARIES_TO_LINK
    ${libconsensus}
    ${libcsma}
    ${libpoint-to-point}
    ${libinternet}
)
