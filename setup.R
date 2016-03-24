REPO = "http://cran.cnr.berkeley.edu/"
install.packages("devtools", repos=REPO)
library(devtools)
install_github(repo = "Surus", username = "Netflix", subdir = "resources/R/RAD")
