from get_info import *
from connection import *
from etl_functions import *
from main import PATH

# Initialize logging setup
setup_logging()

# Get the current date
current_date = datetime.now().date()

# Path to the CSV file within the container
csv_path = PATH + 'datacon.csv'

# Read the CSV file into a DataFrame
data = pd.read_csv(csv_path)

# List of models to be used
modelos = ['gnuhealth.death_certificate', 'gnuhealth.patient.disease',
           'gnuhealth.inpatient.registration', 'gnuhealth.patient.pregnancy',
           'gnuhealth.surgery']

# Convert rows of the DataFrame to a list
con_data = rows_tolist(data)

# Establish connections and retrieve all information for the specified models
all_info = all_connec(con_data, modelos)

# Unpack the records into two separate groups
records1, records2 = all_info

# Unpack individual records from each group
dea1, dise1, reg1, new1, surge1 = records1
dea2, dise2, reg2, new2, surge2 = records2

# Log the connection status
logging.info(f"connecting records")
print('connecting records')

# Retrieve information related to death certificates
deat1, necropsy1, deathill1 = info_death(dea1)
deat2, necropsy2, deathill2 = info_death(dea2)

'''
# Retrieve information related to diseases
dise1_c, dise1_s, dise1_r = info_disease(dise1)
dise2_c, dise2_s, dise2_r = info_disease(dise2)

# Retrieve information related to inpatient registration
bedh1, bedicu1, dish1, dispc1, ocu1, tuover1, esta1 = info_registration(reg1)
bedh2, bedicu2, dish2, dispc2, ocu2, tuover2, esta2 = info_registration(reg2)
'''

# Retrieve information related to newborns
newb1, newbdied1, born1, cae1, vag1 = info_newborn(new1)
newb2, newbdied2, born2, cae2, vag2 = info_newborn(new2)

# Retrieve information related to surgeries
surg1, surgdied1, surgalive1, weekend_surg1, weekend_surgalive1 = info_surgery(surge1)
surg2, surgdied2, surgalive2, weekend_surg2, weekend_surgalive2 = info_surgery(surge2)

# Log that information has been obtained from all models
logging.info(f"info obtained from all models")
print('info obtained from all models')

# Calculate and store mortality rates for surgeries
morta_surgical1 = dict_in_list(dict_payload(div_ind(to_week(sorteddict(transf_dict(surgalive1))), to_week(sorteddict(transf_dict(surg1)))), resultp[0]))
morta_surgical2 = dict_in_list(dict_payload(div_ind(to_week(sorteddict(transf_dict(surgalive2))), to_week(sorteddict(transf_dict(surg2)))), resultp[0]))
logging.info(f"morta_surgical")

# Calculate and store surgery indices
index_surgery1 = dict_in_list(dict_payload(percent_ind(to_week(sorteddict(transf_dict(surg1))), to_week(sorteddict(transf_dict(surg1)))), resulto[0]))
index_surgery2 = dict_in_list(dict_payload(percent_ind(to_week(sorteddict(transf_dict(surg2))), to_week(sorteddict(transf_dict(surg2)))), resulto[0]))
logging.info(f"index_surgery")

# Calculate and store surgery episodes
episode_surgery1 = dict_in_list(dict_payload(to_week(sorteddict(transf_dict(surg1))), resultu[0]))
episode_surgery2 = dict_in_list(dict_payload(to_week(sorteddict(transf_dict(surg1))), resultu[0]))
logging.info(f"episode_surgery")

'''
# Calculate and store turnover rates
turnover1 = dict_in_list(dict_payload(div_ind(to_week(sorteddict(transf_dict(tuover1))), to_week(sorteddict(transf_dict(bedh1)))), resultx[0]))
turnover2 = dict_in_list(dict_payload(div_ind(to_week(sorteddict(transf_dict(tuover2))), to_week(sorteddict(transf_dict(bedh2)))), resultx[0]))
logging.info(f"turnover")

# Calculate and store average stays
average1 = stay(transfor(esta1, resultz[0]), transformar_lista(tuover1, resultz[0]), resultz[0])
average2 = stay(transfor(esta2, resultz[0]), transformar_lista(tuover2, resultz[0]), resultz[0])
logging.info(f"average")
'''

# Calculate and store weekend surgery rates
weekend_surgical1 = dict_in_list(dict_payload(div_ind(res_ind(to_week(sorteddict(transf_dict(weekend_surg1))), to_week(sorteddict(transf_dict(weekend_surgalive1)))), to_week(sorteddict(transf_dict(weekend_surg1)))), results[0]))
weekend_surgical2 = dict_in_list(dict_payload(div_ind(res_ind(to_week(sorteddict(transf_dict(weekend_surg2))), to_week(sorteddict(transf_dict(weekend_surgalive2)))), to_week(sorteddict(transf_dict(weekend_surg2)))), results[0]))
logging.info(f"weekend_surgical")

# Calculate and store cesarean rates
cesarean_rate1 = dict_in_list(dict_payload(percent_ind(to_week(sorteddict(transf_dict(cae1))), to_week(sorteddict(transf_dict(born1)))), resultq[0]))
cesarean_rate2 = dict_in_list(dict_payload(percent_ind(to_week(sorteddict(transf_dict(cae2))), to_week(sorteddict(transf_dict(born2)))), resultq[0]))
logging.info(f"cesarean_rate")

# Calculate and store vaginal delivery rates
vaginal_rate1 = dict_in_list(dict_payload(percent_ind(to_week(sorteddict(transf_dict(vag1))), to_week(sorteddict(transf_dict(born1)))), resultr[0]))
vaginal_rate2 = dict_in_list(dict_payload(percent_ind(to_week(sorteddict(transf_dict(vag2))), to_week(sorteddict(transf_dict(born2)))), resultr[0]))
logging.info(f"vaginal_rate")

# Calculate and store newborn rates
newborn_rate1 = dict_in_list(dict_payload(percent_ind(to_week(sorteddict(transf_dict(newb1))), to_week(sorteddict(transf_dict(born1)))), resultn[0]))
newborn_rate2 = dict_in_list(dict_payload(percent_ind(to_week(sorteddict(transf_dict(newb2))), to_week(sorteddict(transf_dict(born2)))), resultn[0]))
logging.info(f"newborn_rate")

# Calculate and store autopsy rates
autopsy_rate1 = dict_in_list(dict_payload(percentmil_ind(to_week(sorteddict(transf_dict(necropsy1))), to_week(sorteddict(transf_dict(deat1)))), resultm[0]))
autopsy_rate2 = dict_in_list(dict_payload(percentmil_ind(to_week(sorteddict(transf_dict(necropsy2))), to_week(sorteddict(transf_dict(deat2)))), resultm[0]))
logging.info(f"autopsy_rate")

# Calculate and store gross mortality rates
gross_mortality1 = dict_in_list(dict_payload(percentmil_ind(to_week(sorteddict(transf_dict(deat1))), to_week(sorteddict(transf_dict(deat1)))), resultl[0]))
gross_mortality2 = dict_in_list(dict_payload(percentmil_ind(to_week(sorteddict(transf_dict(deat2))), to_week(sorteddict(transf_dict(deat2)))), resultl[0]))
logging.info(f"gross_mortality")

# Calculate and store daily death information
DailyDeath1 = dictio_to_list(result1, trans_dict(deathill1, names_illnes))
DailyDeath2 = dictio_to_list(result1, trans_dict(deathill2, names_illnes))
logging.info(f"DailyDeath")

# Calculate and store weekly death information
d1 = dictio_to_list(resultj, weekly(sortedict(trans_dict(deathill1, names_illnes))))
d2 = dictio_to_list(resultj, weekly(sortedict(trans_dict(deathill2, names_illnes))))
logging.info(f"D")

'''
# Calculate and store occupancy rates
b1 = dictio_to_list(resultt, weekly(sortedict(trans_dict(ocu1, names_illnes))))
b2 = dictio_to_list(resultt, weekly(sortedict(trans_dict(ocu2, names_illnes))))
logging.info(f"B")

# Calculate and store daily confirmed cases
DailyConfirmedt1 = dictio_to_list(result2, trans_dict(dise1_c, names_illnes))
DailyConfirmedt2 = dictio_to_list(result2, trans_dict(dise2_c, names_illnes))
logging.info(f"DailyConfirmed")

# Calculate and store weekly confirmed cases
c1 = dictio_to_list(resultcon, weekly(sortedict(trans_dict(dise1_c, names_illnes))))
c2 = dictio_to_list(resultcon, weekly(sortedict(trans_dict(dise2_c, names_illnes))))
logging.info(f"C")

# Calculate and store daily recovered cases
DailyRecoveredt1 = dictio_to_list(result3, trans_dict(dise1_r, names_illnes))
DailyRecoveredt2 = dictio_to_list(result3, trans_dict(dise2_r, names_illnes))
logging.info(f"DailyRecovered")

# Calculate and store weekly recovered cases
r1 = dictio_to_list(resultk, weekly(sortedict(trans_dict(dise1_r, names_illnes))))
r2 = dictio_to_list(resultk, weekly(sortedict(trans_dict(dise2_r, names_illnes))))
logging.info(f"R")

# Calculate and store weekly serious cases
s1 = dictio_to_list(resulti, weekly(sortedict(trans_dict(dise1_s, names_illnes))))
s2 = dictio_to_list(resulti, weekly(sortedict(trans_dict(dise2_s, names_illnes))))
logging.info(f"S")

# Calculate and store weekly discharges
a1 = dict_to_list(resultd, weekly(sortedict(trans_dict(dish1, names_illnes))))
a2 = dict_to_list(resultd, weekly(sortedict(trans_dict(dish2, names_illnes))))
logging.info(f"A")

# Calculate and store primary health care data
phc1 = dict_to_list(resulte, weekly(sortedict(trans_dict(dispc1, names_illnes))))
phc2 = dict_to_list(resulte, weekly(sortedict(trans_dict(dispc2, names_illnes))))
logging.info(f"PHC")

# Calculate and store bed occupancy rates in hospitals
tb1 = dict_to_list(resultf, weekly(sortedict(trans_dict(bedh1, names_illnes))))
tb2 = dict_to_list(resultf, weekly(sortedict(trans_dict(bedh2, names_illnes))))
logging.info(f"TB")

# Calculate and store ICU bed occupancy rates
ti1 = dict_to_list(resultg, weekly(sortedict(trans_dict(bedicu1, names_illnes))))
ti2 = dict_to_list(resultg, weekly(sortedict(trans_dict(bedicu2, names_illnes))))
logging.info(f"TI")

# Calculate and store solved cases
solved1 = dict_to_list(resulth, sumar(weekly(sortedict(trans_dict(dise1_r, names_illnes))), weekly(sortedict(trans_dict(deathill1, names_illnes)))))
solved2 = dict_to_list(resulth, sumar(weekly(sortedict(trans_dict(dise2_r, names_illnes))), weekly(sortedict(trans_dict(deathill2, names_illnes)))))
logging.info(f"Solved")

# Calculate and store daily solved cases
DailySolvedt1 = dict_to_list(result4, sumar(sortedict(trans_dict(dise1_r, names_illnes)), sortedict(trans_dict(deathill1, names_illnes))))
DailySolvedt2 = dict_to_list(result4, sumar(sortedict(trans_dict(dise2_r, names_illnes)), sortedict(trans_dict(deathill2, names_illnes))))
logging.info(f"DailySolved")

# Calculate and store death percentages
DeathsPercentage1 = dict_to_list(resulth, porcentaje(sortedict(trans_dict(deathill1, names_illnes)), sortedict(trans_dict(dise1_c, names_illnes))))
DeathsPercentage2 = dict_to_list(resulth, porcentaje(sortedict(trans_dict(deathill2, names_illnes)), sortedict(trans_dict(dise2_c, names_illnes))))
logging.info(f"DeathsPercentage")

# Calculate and store recovery percentages
RecoveredPercentage1 = dict_to_list(resulth, porcentaje(sortedict(trans_dict(dise1_r, names_illnes)), sortedict(trans_dict(dise1_c, names_illnes))))
RecoveredPercentage2 = dict_to_list(resulth, porcentaje(sortedict(trans_dict(dise2_r, names_illnes)), sortedict(trans_dict(dise2_c, names_illnes))))
logging.info(f"RecoveredPercentage")
'''

# Calculate hospitalization percentage for first set of data
hPOR1 = dict_to_list(
    resultb,
    dividir(
        sumar_tres_diccionarios(
            sortedict(trans_dict(dise1_c, names_illnes)),
            sortedict(trans_dict(deathill1, names_illnes)),
            sortedict(trans_dict(dise1_r, names_illnes))
        ),
        sortedict(trans_dict(bedh1, names_illnes))
    )
)

# Calculate hospitalization percentage for second set of data
hPOR2 = dict_to_list(
    resultb,
    dividir(
        sumar_tres_diccionarios(
            sortedict(trans_dict(dise2_c, names_illnes)),
            sortedict(trans_dict(deathill2, names_illnes)),
            sortedict(trans_dict(dise2_r, names_illnes))
        ),
        sortedict(trans_dict(bedh2, names_illnes))
    )
)
logging.info(f"hPOR")

# Calculate ICU percentage for first set of data
icuPOR1 = dict_to_list(
    resultc,
    dividir(
        sumar_tres_diccionarios(
            sortedict(trans_dict(dise1_c, names_illnes)),
            sortedict(trans_dict(deathill1, names_illnes)),
            sortedict(trans_dict(dise1_r, names_illnes))
        ),
        sortedict(trans_dict(bedicu1, names_illnes))
    )
)

# Calculate ICU percentage for second set of data
icuPOR2 = dict_to_list(
    resultc,
    dividir(
        sumar_tres_diccionarios(
            sortedict(trans_dict(dise2_c, names_illnes)),
            sortedict(trans_dict(deathill2, names_illnes)),
            sortedict(trans_dict(dise2_r, names_illnes))
        ),
        sortedict(trans_dict(bedicu2, names_illnes))
    )
)
logging.info(f"icuPOR")

# Calculate accumulated hospital stay index for first set of data
aHsi1 = dict_to_list(
    resulth,
    porcentaje(
        sumar(
            sortedict(trans_dict(dise1_r, names_illnes)),
            sortedict(trans_dict(deathill1, names_illnes))
        ),
        sortedict(trans_dict(dise1_c, names_illnes))
    )
)

# Calculate accumulated hospital stay index for second set of data
aHsi2 = dict_to_list(
    resulth,
    porcentaje(
        sumar(
            sortedict(trans_dict(dise1_r, names_illnes)),
            sortedict(trans_dict(deathill1, names_illnes))
        ),
        sortedict(trans_dict(dise2_c, names_illnes))
    )
)
logging.info(f"aHsi")

# Calculate discharge hospital stay index for first set of data
dHsi1 = dict_to_list(
    resulth,
    dividir(
        sumar(
            sortedict(trans_dict(dise1_r, names_illnes)),
            sortedict(trans_dict(deathill1, names_illnes))
        ),
        sortedict(trans_dict(dise1_c, names_illnes))
    )
)

# Calculate discharge hospital stay index for second set of data
dHsi2 = dict_to_list(
    resulth,
    dividir(
        sumar(
            sortedict(trans_dict(dise1_r, names_illnes)),
            sortedict(trans_dict(deathill1, names_illnes))
        ),
        sortedict(trans_dict(dise2_c, names_illnes))
    )
)
logging.info(f"dHsi")

# Calculate active cases for first set of data
activecases1 = dict_to_list(
    result5,
    sumar_tres_diccionarios(
        sortedict(trans_dict(dise1_c, names_illnes)),
        sortedict(trans_dict(deathill1, names_illnes)),
        sortedict(trans_dict(dise1_r, names_illnes))
    )
)

# Calculate active cases for second set of data
activecases2 = dict_to_list(
    result5,
    sumar_tres_diccionarios(
        sortedict(trans_dict(dise2_c, names_illnes)),
        sortedict(trans_dict(deathill1, names_illnes)),
        sortedict(trans_dict(dise2_r, names_illnes))
    )
)
logging.info(f"activecases")

# Calculate pre-active cases for first set of data
PreActiveCases1 = dict_to_list(
    result6,
    sumar_tres_diccionarios(
        sortedict(trans_dict(dise1_c, names_illnes)),
        sortedict(trans_dict(deathill1, names_illnes)),
        sortedict(trans_dict(dise1_r, names_illnes))
    )
)

# Calculate pre-active cases for second set of data
PreActiveCases2 = dict_to_list(
    result6,
    sumar_tres_diccionarios(
        sortedict(trans_dict(dise2_c, names_illnes)),
        sortedict(trans_dict(deathill1, names_illnes)),
        sortedict(trans_dict(dise2_r, names_illnes))
    )
)
logging.info("PreActiveCases")

'''
# Print a message indicating list calculations are complete
print('list calculated')
'''

# Add calculated columns to the DataFrame
data['Nueva_Columna6'] = [s1, s2]
data['Nueva_Columna7'] = [c1, c2]
data['Nueva_Columna9'] = [r1, r2]
data['Nueva_Columnaf'] = [DailyConfirmedt1, DailyConfirmedt2]
data['Nueva_Columnah'] = [DailyRecoveredt1, DailyRecoveredt2]
data['Nueva_Columnaa'] = [a1, a2]
data['Nueva_Columnab'] = [phc1, phc2]
data['Nueva_Columnac'] = [tb1, tb2]
data['Nueva_Columnad'] = [ti1, ti2]
data['Nueva_Columnae'] = [solved1, solved2]
data['Nueva_Columnai'] = [DailySolvedt1, DailySolvedt2]
data['Nueva_Columnaj'] = [DeathsPercentage1, DeathsPercentage2]
data['Nueva_Columnak'] = [RecoveredPercentage1, RecoveredPercentage2]
data['Nueva_Columnal'] = [aHsi1, aHsi2]
data['Nueva_Columnam'] = [dHsi1, dHsi2]
data['Nueva_Columnan'] = [hPOR1, hPOR2]
data['Nueva_Columnao'] = [icuPOR1, icuPOR2]
data['Nueva_Columnap'] = [activecases1, activecases2]
data['Nueva_Columnaq'] = [PreActiveCases1, PreActiveCases2]
data['Nueva_Columnar'] = [b1, b2]

'''
data['Nueva_Columna8'] = [d1, d2]
data['Nueva_Columnag'] = [DailyDeath1, DailyDeath2]
data['Nueva_Columnas'] = [vaginal_rate1, vaginal_rate2]
data['Nueva_Columnat'] = [cesarean_rate1, cesarean_rate2]
data['Nueva_Columnau'] = [weekend_surgical1, weekend_surgical2]
data['Nueva_Columnaw'] = [episode_surgery1, episode_surgery2]
#data['Nueva_Columnay'] = [turnover1, turnover2]
#data['Nueva_Columnaz'] = [average1, average2]
'''

# Convert the DataFrame rows to a list for further processing
con_data = rows_tolist(data)
