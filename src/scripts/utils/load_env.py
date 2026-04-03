# Standard librairies
import os

# External packages
from dotenv import load_dotenv


load_dotenv()


#################
#### CHEMINS ####
#################
PATH_DATA = os.getenv('PATH_DATA')
#### !CHEMINS ####



##################
#### FICHIERS ####
##################

#### !FICHIERS ####


##################
#### VARIABLE ####
##################
DATA_ANN_DEF_YR_START = int(os.getenv('DATA_ANN_DEF_YR_START', 2012))
DATA_ANN_DEF_YR_END = int(os.getenv('DATA_ANN_DEF_YR_END', 2024))
## Docker ##

#### !VARIABLE ####


###############
#### LIENS ####
###############
#### LIENS ####
###############
SQR_TX_METRO = os.getenv('SQR_TX_METRO')
SQR_TN_METRO = os.getenv('SQR_TN_METRO')
#### !LIENS ####
