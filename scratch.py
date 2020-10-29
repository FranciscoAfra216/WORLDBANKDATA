import pandas as pd
import numpy as np


from bs4 import BeautifulSoup
import requests
import world_bank_data as wb


carsdf = pd.read_csv("./vehicles.csv")
carsdf.head(10)

cleaneddf=carsdf.fillna("filled")

dfcars_selection = carsdf[["make","id","model","fuelType","fuelType1","fuelType2","evMotor", "cityE","co2","co2TailpipeAGpm","co2TailpipeGpm","comb08", "combA08", "combE","highway08","highwayA08","highwayE","UCity","UCityA","UHighway","UHighwayA" ]]

dfselection = cleaneddf[["make","id","model","year","fuelType","evMotor","cityE","co2","co2TailpipeGpm","comb08", "combE","highway08","highwayE","UCity","UHighway"]]

dfselection[dfselection["fuelType"]=="Electricity"][["make","model"]]

dfselection[dfselection["fuelType"]=="Electricity"]

motortype = dfselection[(dfselection["fuelType"]=="Electricity") & (dfselection["year"]>2018)]["evMotor"].value_counts()

model_el_name=dfselection[(dfselection["fuelType"]=="Electricity") & (dfselection["year"]>2018)][["make","model"]]

allcars_2018 = dfselection[dfselection["year"]>2018]

allcars_2018["co2_per_km"]=allcars_2018["co2TailpipeGpm"]/1.60934 #already done. commented to not rerun
allcars_2018["cityE_100km"]=allcars_2018["cityE"]/1.60934
#MPG transform / by 2.3521458
allcars_2018["comb08_per_km"]=allcars_2018["comb08"]/2.3521458 #already done. commented to not rerun
#convert in kms
allcars_2018["combE_per_km"]=allcars_2018["combE"]/1.60934 #already done. commented to not rerun
#MPG transform
allcars_2018["highway08_per_lkm"]=allcars_2018["highway08"]/2.3521458 #already done. commented to not rerun
#convert in kms
allcars_2018["highwayE_per_km"]=allcars_2018["highwayE"]/1.60934 #already done. commented to not rerun
#MPG transform
allcars_2018["UCity_per_lkm"]=allcars_2018["UCity"]/2.3521458 #already done. commented to not rerun
#MPG transform
allcars_2018["UHighway_per_lkm"]=allcars_2018["UHighway"]/2.3521458 #already done. commented to not rerun
#convert in kms
allcars_2018["co2_km"]=allcars_2018["co2"]/1.60934 #already done. commented to not rerun

allcars_2018= allcars_2018.drop(columns=["cityE","co2","co2TailpipeGpm", 'comb08', 'combE', 'highway08', 'highwayE', 'UCity','UHighway'])
cars2018_c=allcars_2018
cars2018_c=cars2018_c.reset_index()
cars2018_c=cars2018_c.drop(columns='index')

#FETCHING FOR VARIOUS TABLES

elect_prod_renew_sourc_per = pd.DataFrame(wb.get_series('EG.ELC.RNWX.ZS', mrv=1))
elect_prod_renew_sourc_kwh = pd.DataFrame(wb.get_series('EG.ELC.RNWX.KH', mrv=1))
elect_prod_oil_sourc = pd.DataFrame(wb.get_series('EG.ELC.PETR.ZS', mrv=1))
elect_prod_nucl_sourc = pd.DataFrame(wb.get_series('EG.ELC.NUCL.ZS', mrv=1))
elect_prod_coal_sourc = pd.DataFrame(wb.get_series('EG.ELC.COAL.ZS', mrv=1))
elect_prod_oilgascoal_sourc = pd.DataFrame(wb.get_series('EG.ELC.FOSL.ZS', mrv=1))
elect_prod_natgas_sourc = pd.DataFrame(wb.get_series('EG.ELC.NGAS.ZS', mrv=1))
elect_prod_hydro_sourc = pd.DataFrame(wb.get_series('EG.ELC.HYRO.ZS', mrv=1))
elect_power_transm_loss = pd.DataFrame(wb.get_series('EG.ELC.LOSS.ZS', mrv=1))
elect_access = pd.DataFrame(wb.get_series('EG.ELC.ACCS.ZS', mrv=1))
elect_access_urban = pd.DataFrame(wb.get_series('EG.ELC.ACCS.UR.ZS', mrv=1))
elect_access_rural = pd.DataFrame(wb.get_series('EG.ELC.ACCS.RU.ZS', mrv=1))

#REMOVING COLUMNS FROM INDEXES
elect_prod_renew_sourc_per.reset_index(inplace=True)
elect_prod_renew_sourc_kwh.reset_index(inplace=True)
elect_prod_oil_sourc.reset_index(inplace=True)
elect_prod_nucl_sourc.reset_index(inplace=True)
elect_prod_coal_sourc.reset_index(inplace=True)
elect_prod_oilgascoal_sourc.reset_index(inplace=True)
elect_prod_natgas_sourc.reset_index(inplace=True)
elect_prod_hydro_sourc .reset_index(inplace=True)
elect_power_transm_loss.reset_index(inplace=True)
elect_access.reset_index(inplace=True)
elect_access_urban.reset_index(inplace=True)
elect_access_rural.reset_index(inplace=True)

#RENAMING VALUE COLUMNS
elect_prod_renew_sourc_per=elect_prod_renew_sourc_per.rename(columns={"EG.ELC.RNWX.ZS": "Percentage"})
elect_prod_renew_sourc_kwh=elect_prod_renew_sourc_kwh.rename(columns={"EG.ELC.RNWX.KH": "KWH"})
elect_prod_oil_sourc=elect_prod_oil_sourc.rename(columns={"EG.ELC.PETR.ZS": "Percentage"})
elect_prod_nucl_sourc=elect_prod_nucl_sourc.rename(columns={"EG.ELC.NUCL.ZS": "Percentage"})
elect_prod_coal_sourc=elect_prod_coal_sourc.rename(columns={"EG.ELC.COAL.ZS": "Percentage"})
elect_prod_oilgascoal_sourc=elect_prod_oilgascoal_sourc.rename(columns={"EG.ELC.FOSL.ZS": "Percentage"})
elect_prod_natgas_sourc = elect_prod_natgas_sourc.rename(columns={"EG.ELC.NGAS.ZS": "Percentage"})
elect_prod_hydro_sourc = elect_prod_hydro_sourc.rename(columns={"EG.ELC.HYRO.ZS": "Percentage"})
elect_power_transm_loss=elect_power_transm_loss.rename(columns={"EG.ELC.LOSS.ZS": "Percentage"})
elect_access=elect_access.rename(columns={"EG.ELC.ACCS.ZS": "Percentage"})
elect_access_urban=elect_access_urban.rename(columns={"EG.ELC.ACCS.UR.ZS": "Percentage"})
elect_access_rural=elect_access_rural.rename(columns={"EG.ELC.ACCS.RU.ZS": "Percentage"})

#DROPPING DUMMY COLUMNS
elect_prod_renew_sourc_per=elect_prod_renew_sourc_per.drop(columns=['Series'])
elect_prod_renew_sourc_kwh=elect_prod_renew_sourc_kwh.drop(columns=['Series'])
elect_prod_oil_sourc=elect_prod_oil_sourc.drop(columns=['Series'])
elect_prod_nucl_sourc=elect_prod_nucl_sourc.drop(columns=['Series'])
elect_prod_coal_sourc=elect_prod_coal_sourc.drop(columns=['Series'])
elect_prod_oilgascoal_sourc=elect_prod_oilgascoal_sourc.drop(columns=['Series'])
elect_prod_natgas_sourc = elect_prod_natgas_sourc.drop(columns=['Series'])
elect_prod_hydro_sourc = elect_prod_hydro_sourc.drop(columns=['Series'])
elect_power_transm_loss=elect_power_transm_loss.drop(columns=['Series'])
elect_access=elect_access.drop(columns=['Series'])
elect_access_urban=elect_access_urban.drop(columns=['Series'])
elect_access_rural=elect_access_rural.drop(columns=['Series'])

result=pd.merge(elect_access, elect_access_rural, on="Country")

table_elec_access=pd.merge(result,elect_access_urban, on="Country")

table_elec_access=table_elec_access.rename(columns={"Percentage_x": "Electricity Access WW", "Percentage_y":"Electricity Access Rural" , "Percentage":"Electricity Access Urban"})

table_elec_access=table_elec_access.drop(columns=['Year','Year_y'])

table_elec_access=table_elec_access.rename(columns={"Year_x": "Year"})


combination=pd.merge(elect_prod_renew_sourc_per, elect_prod_oil_sourc, on="Country")

combination=combination.drop(columns=['Year_y'])

combination=combination.rename(columns={"Year_x": "Year", "Percentage_y":"Oil Sources %" , "Percentage_x":"Renewable Sources %"})

combination2=pd.merge(combination, elect_prod_nucl_sourc, on="Country")

combination2=combination2.drop(columns=['Year_y'])

combination2=combination2.rename(columns={"Percentage": "Nuclear Sources %"})

combination3=pd.merge(combination2, elect_prod_coal_sourc, on="Country")

combination3=combination3.drop(columns=['Year'])

combination4=combination3.rename(columns={"Year_x": "Year", "Percentage": "Coal Sources %"})

combination5=pd.merge(combination4, elect_prod_natgas_sourc, on="Country")

combination5=combination5.drop(columns=['Year_y'])

combination5=combination5.rename(columns={"Percentage": "Natural Gas Sources %", "Year_x": "Year"})

combination6=pd.merge(combination5, elect_prod_hydro_sourc, on="Country")

combination6=combination6.drop(columns=['Year_y'])

table_elec_source_breakdown=combination6.rename(columns={"Percentage": "Hydroelectric Sources %", "Year_x": "Year"})

table_elec_source_breakdown["rest"]= 100-table_elec_source_breakdown.iloc[:,2:8].fillna(0).apply(sum,axis=1)
table_elec_source_breakdown=table_elec_source_breakdown.fillna(0)

# conversion table for c02emission depending on raw material source used

# 20kwh of coal : 20000g
# 20kwh of nuclear *: usually accepted to emit 70% less than diesel so : 3000g
# 20kwh of natural gas : 10000 gCO2/100km
# 20kwh renewable : < 1200g CO2/100km

# kg co2/kwh produced ---those values below are in kg,  . source https://www.eia.gov/tools/faqs/faq.php?id=74&t=11
coal_prod = 1002.439
nuclear_prod = 0.2 * 1002.439  # confirm
natural_gas_prod = 417.305
renewable_prod = 0.2 * 1002.439  # confirm
hydroelectric_prod = 0.2 * 1002.439  # confirm
oil_prod = 957.0799

cars2018_c["model_full"] = cars2018_c["make"] + " " + cars2018_c["model"]

habit_list = []


# ------------------------------------------FUNCTIONS-START------------------------------------------#
def habit_decision():
    habit_driving = input('Where do you usally drive? City, Highway or Both?')

    while (habit_driving != 'City') & (habit_driving != 'Highway') & (habit_driving != 'Both'):
        print('Spell it decently please...')
        habit_driving = input('Where do you usally drive? City, Highway or Both?')

    habit_list.append(habit_driving)
    car_decision_making()


# ------------------------------------------FUNCTIONS------------------------------------------#
country_picks = []


def country_decision_making():
    country1 = input('Pick a country for analysis?')
    while (table_elec_source_breakdown['Country'] == country1).any() == False:
        print('Spell it decently please...')
        country1 = input('Pick a country for analysis?')
    country_picks.append(country1)
    country_decision2 = input('Do you want to pick another country?(Y) or (N)')
    while (country_decision2 != 'Y') & (country_decision2 != 'N'):
        country_decision2 = input('Do you want to pick another car?(Y) or (N)')

    if country_decision2 == 'Y':
        country2 = input('Pick another country?')
        while (table_elec_source_breakdown['Country'] == country2).any() == False:
            print('Spell it decently please...')
            country2 = input('Pick another country?')

        country_picks.append(country2)
        country_decision3 = input('Do you want to pick another country?(Y) or (N)')
        while (country_decision3 != 'Y') & (country_decision3 != 'N'):
            country_decision3 = input('Do you want to pick another car?(Y) or (N)')

        if country_decision3 == 'Y':
            country3 = input('Pick another country?')
            while (table_elec_source_breakdown['Country'] == country3).any() == False:
                print('Spell it decently please...')
                country3 = input('Pick another country?')
            country_picks.append(country3)
            habit_decision()
        elif country_decision3 == 'N':
            habit_decision()

    elif country_decision2 == 'N':
        habit_decision()


# ------------------------------------------FUNCTIONS------------------------------------------#
car_picks = []


def car_decision_making():
    brand1 = input('Pick a brand car for analysis?')
    while (cars2018_c['make'] == brand1).any() == False:
        print('Spell it decently please...')
        brand1 = input('Pick a brand car for analysis?')

    print('\n')
    print(set(cars2018_c[cars2018_c['make'] == brand1]['model']))
    print('\n')

    model1 = input('Pick a model for your ' + brand1 + '!')
    while (cars2018_c['model'] == model1).any() == False:
        print('Spell it decently please...')
        model1 = input('Pick a model for your ' + brand1 + '!')
    car_choice1 = brand1 + ' ' + model1
    car_picks.append(car_choice1)

    more_cars1 = input('Do you want to pick another car?(Y) or (N)')
    while (more_cars1 != 'Y') & (more_cars1 != 'N'):
        more_cars1 = input('Do you want to pick another car?(Y) or (N)')

    if more_cars1 == 'Y':
        brand2 = input('Pick a brand car for analysis?')
        while (cars2018_c['make'] == brand2).any() == False:
            print('Spell it decently please...')
            brand2 = input('Pick a brand car for analysis?')
        print('\n')
        print(set(cars2018_c[cars2018_c['make'] == brand2]['model']))
        print('\n')
        model2 = input('Pick a model for your ' + brand2 + '!')
        while (cars2018_c['model'] == model2).any() == False:
            print('Spell it decently please...')
            model2 = input('Pick a model for your ' + brand2 + '!')
        car_choice2 = brand2 + ' ' + model2
        car_picks.append(car_choice2)

        more_cars2 = input('Do you want to pick another car?(Y) or (N)')
        while (more_cars2 != 'Y') & (more_cars2 != 'N'):
            more_cars2 = input('Do you want to pick another car?(Y) or (N)')

        if more_cars2 == 'Y':
            brand3 = input('Pick a brand car for analysis?')
            while (cars2018_c['make'] == brand3).any() == False:
                print('Spell it decently please...')
                brand3 = input('Pick a brand car for analysis?')
            print('\n')
            print(set(cars2018_c[cars2018_c['make'] == brand3]['model']))
            print('\n')
            model3 = input('Pick a model for your ' + brand3 + '!')
            while (cars2018_c['model'] == model3).any() == False:
                print('Spell it decently please...')
                model3 = input('Pick a model for your ' + brand3 + '!')
            car_choice3 = brand3 + ' ' + model3
            car_picks.append(car_choice3)
            dataframe_result()


        elif more_cars2 == 'N':
            dataframe_result()

    elif more_cars1 == 'N':
        dataframe_result()


def play_product():
    ### WE NEED A BETTER INTRO####
    print(
        'Welcome to our analysis, where we help you make your own decision based on facts CO2 Emissions per Country and Car!')
    print('\n')
    country_decision_making()


def dataframe_result():
    dict_car = {}
    for model in car_picks:
        co2values = []
        for country in country_picks:
            if cars2018_c["fuelType"][cars2018_c["model_full"] == model].iloc[0] != "Electricity":
                co2emission = (cars2018_c["co2_per_km"][
                    cars2018_c["model_full"] == model]).mean()  # gives an answer in GRAM of co2 per KILOÑEMETER
                co2values.append(co2emission)
            else:
                oil_source = \
                table_elec_source_breakdown[table_elec_source_breakdown["Country"] == country]["Oil Sources %"].iloc[
                    0] / 100
                nuclear_source = table_elec_source_breakdown[table_elec_source_breakdown["Country"] == country][
                                     "Nuclear Sources %"].iloc[0] / 100
                coal_source = \
                table_elec_source_breakdown[table_elec_source_breakdown["Country"] == country]["Coal Sources %"].iloc[
                    0] / 100
                naturalgas_source = table_elec_source_breakdown[table_elec_source_breakdown["Country"] == country][
                                        "Natural Gas Sources %"].iloc[0] / 100
                Hydro_source = table_elec_source_breakdown[table_elec_source_breakdown["Country"] == country][
                                   "Hydroelectric Sources %"].iloc[0] / 100
                Renew_source = table_elec_source_breakdown[table_elec_source_breakdown["Country"] == country][
                                   "Renewable Sources %"].iloc[0] / 100
                rest_source = \
                table_elec_source_breakdown[table_elec_source_breakdown["Country"] == country]["rest"].iloc[0] / 100

                if habit_list[0] == "City":
                    kwhperkm = (cars2018_c["cityE_100km"][cars2018_c["model_full"] == model] / 100).mean()

                    co2emission_elec = kwhperkm * coal_source * coal_prod + kwhperkm * oil_source * oil_prod + kwhperkm * nuclear_prod * nuclear_source + kwhperkm * natural_gas_prod * naturalgas_source + kwhperkm * renewable_prod * Renew_source + kwhperkm * hydroelectric_prod * Hydro_source

                    co2emission_elec += rest_source * co2emission_elec

                    co2emission_elec_inclbatt = co2emission_elec + 52  # calculation explained in documents attached: battery co2

                    co2values.append(co2emission_elec_inclbatt)

                if habit_list[0] == "Highway":
                    kwhperkm = (cars2018_c["highwayE_per_km"][cars2018_c["model_full"] == model] / 100).mean()
                    co2emission_elec = kwhperkm * coal_source * coal_prod + kwhperkm * oil_source * oil_prod + kwhperkm * nuclear_prod * nuclear_source + kwhperkm * natural_gas_prod * naturalgas_source + kwhperkm * renewable_prod * Renew_source + kwhperkm * hydroelectric_prod * Hydro_source
                    co2emission_elec += rest_source * co2emission_elec
                    co2emission_elec_inclbatt = co2emission_elec + 52
                    co2values.append(co2emission_elec_inclbatt)

                if habit_list[0] == "Both":
                    kwhperkm = (cars2018_c["combE_per_km"][cars2018_c["model_full"] == model] / 100).mean()
                    co2emission_elec = kwhperkm * coal_source * coal_prod + kwhperkm * oil_source * oil_prod + kwhperkm * nuclear_prod * nuclear_source + kwhperkm * natural_gas_prod * naturalgas_source + kwhperkm * renewable_prod * Renew_source + kwhperkm * hydroelectric_prod * Hydro_source
                    co2emission_elec += rest_source * co2emission_elec
                    co2emission_elec_inclbatt = co2emission_elec + 52
                    co2values.append(co2emission_elec_inclbatt)

        dict_car[model] = co2values

    df_car = pd.DataFrame.from_dict(dict_car).T
    df_car.columns = country_picks
    return df_car
# ------------------------------------------FUNCTIONS-END------------------------------------------#

dataframe_result()
