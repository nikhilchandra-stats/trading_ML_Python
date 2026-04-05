create_lm_formula <- function(
    dependant = "Weekly_Change",
    independant = c("AIG Construction Index", "AIG Manufacturing Index", "AIG Services Index", "Private Sector Credit" ,
                    "Producer Price Index",  "Building Permits", "NAB Business Confidence Index",
                    "Private Sector Credit", "Retail Sales"),
    interaction_terms = NULL
) {

  independant_quoted <- independant %>%
    map( ~ paste0("`",.x,"`")) %>% unlist() %>% as.character()

  pluses <- independant_quoted %>% paste(collapse = " + ")

  final_formula <-glue::glue("{dependant} ~ {pluses}")

  if(!is.null(interaction_terms)) {

    interacts <-
      c("state_space_lowest_middle", "state_space_below_max_3") %>%
      paste(collapse = "*")

    final_formula <-
      paste(c(final_formula, interacts), " + ")

  }

  final_formula <- final_formula %>% as.formula()

  return(final_formula)
}

create_lm_formula_no_space <- function(
    dependant = "Weekly_Change",
    independant = c("AIG Construction Index", "AIG Manufacturing Index", "AIG Services Index", "Private Sector Credit" ,
                    "Producer Price Index",  "Building Permits", "NAB Business Confidence Index",
                    "Private Sector Credit", "Retail Sales")
) {

  pluses <- independant %>% paste(collapse = " + ")

  final_formula <-glue::glue("{dependant} ~ {pluses}") %>% as.character() %>%  as.formula()

  # form_test <- glue::glue("{dependant} ~ {independant[1]}") %>% as.character()
  #
  # form_test %>% as.formula()
  #
  # for (i in 1:length(independant)) {
  #
  #   form_test <- glue::glue("{form_test} + {independant[i]}") %>% as.character()
  #   form_test %>% as.formula()
  #
  #
  # }

  return(final_formula)
}
