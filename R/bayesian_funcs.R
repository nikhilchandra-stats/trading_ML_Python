get_pois_calc <- function(asset_data = asset_data_combined,
                          tau = 10,
                          starting_scale = 0.5,
                          starting_shape = 10,
                          prior_weight = 10) {

  rgamma(n = 100, scale = 0.5, shape = 10) %>% mean(na.rm = T)

  xx <- alpha*theta
  alpha = xx/theta

  prepped_data <-
    asset_data %>%
    group_by(Asset) %>%
    mutate(
      group_block = floor(row_number()/tau)
    ) %>%
    group_by(Asset, group_block) %>%
    mutate(
      change_bin =
        case_when(
          Open - Price > 0 ~ 1,
          Open - Price <= 0 ~ 0
          )
    ) %>%
    group_by(Asset, group_block) %>%
    mutate(
      running_bin = cumsum(change_bin)
    )

  pois_estimates <-
    prepped_data %>%
    group_by(Asset, group_block) %>%
    summarise(running_bin = sum(change_bin, na.rm = T),
              tau = tau) %>%
    # mutate(
    #   running_mean = slider::slide_dbl(.x = running_bin, )
    # )
    mutate(
      prior_scale = lag(running_bin, 2)/tau,
      prior_shape = tau,
      posterior_scale = prior_scale
    )

}
