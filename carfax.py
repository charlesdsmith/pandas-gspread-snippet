#takes the carfax for a certain VIN from one sheet, then locates that same VIN in another sheet and updates
#that record with the VINs corresponding carfax info
#carfax information is in an array of 6 items

import sys
import os
moduleDir = os.path.abspath(os.path.join(__file__, "../../Helpers"))
sys.path.insert(0, moduleDir)

from gspread_dataframe import get_as_dataframe, set_with_dataframe
from gspread import exceptions
import pandas as pd


try:
    import FromMailToGoogle.inputToSheets as inputToSheets
except:
    import inputToSheets as inputToSheets
try:
    import uploadToDrive as uploadToDrive
except:
    import FromMailToGoogle.uploadToDrive as uploadToDrive

from MailRobot.FromMailToGoogle import carfaxReports


GoogleDriveAuth = uploadToDrive.authenticate()
gc = inputToSheets.startAuthorization()
theBigSheet = inputToSheets.startWorksheet(gc)
#theBOSSheet = inputToSheets.startTradeRevWorksheet(gc)
registrationSheet = inputToSheets.startRegistrationsFriday(gc)
carfaxSheet = inputToSheets.startCarfaxSheet(gc)

def carFax():
    #vin should return multiple values, the info from the carfax
    #test1, test2, test3, test4, test5, test6 = carFaxanalyzer.analyze(vin)

    registration_df = get_as_dataframe(registrationSheet).dropna(how='all')
    registration_vins = registration_df['VIN(17)'].values
    carfax_df = get_as_dataframe(carfaxSheet)
    carfax_vins = []
    registration_info = []


    for row, cell in enumerate(carfax_df['UNIV KEY'].values):
        carfax_vins.append((row, cell))

    carfaxtoken = carfaxReports.getTokens()
    for row, cell in enumerate(registration_vins):
        carfax = carfaxReports.getImportantValues(cell, carfaxtoken)

        registration_info.append((row, cell, carfax[0], carfax[1], carfax[2], carfax[3], carfax[4], carfax[5]))

    matching_vins = [i for i in carfax_vins for info in registration_info if i[1] == info[1]]
    final_info = []

    #find the vins that are in the carfax sheet and the registrations friday sheet, then combine the info into a tuple
    #so to preserve the row number of the vin in the carfax sheet so it can be updated
    for i in matching_vins:
        for x in registration_info:
            if i[1] == x[1]:
                final_info.append(i+x[1:8])

    for vin in final_info:
        carfax_df.loc[vin[0], 'carfax Totalloss'] = vin[3]
        carfax_df.loc[vin[0], 'carfax StructuralDamage'] = vin[4]
        carfax_df.loc[vin[0], 'carfax airbags'] = vin[5]
        carfax_df.loc[vin[0], 'carfax odometer'] = vin[6]
        carfax_df.loc[vin[0], 'carfax accident'] = vin[7]
        carfax_df.loc[vin[0], 'carfax recall'] = vin[8]

    set_with_dataframe(carfaxSheet, carfax_df)
