#r@"..\sa2-dlls\common.dll"



#load ".\settingsProduction.fsx"


open SA2.Common.Table_data_fields


let dataFields = 

    [

        // format is : ( SA2 Name , BB Name )

        ( "fixedPayFreq" , "fixedPayFreq" )

        ( "floatIndex" , "floatIndex" )

        ( "floatResetFreq" , "floatResetFreq" )

        ( "floatDayCount" , "floatDayCount" )

        ( "oisFloatPayDelay" , "oisFloatPayDelay" )

        ( "floatPayFreq" , "floatPayFreq" )


    ]

    |> Map.ofList


// execute. prints to the screen

update_data_fields dataFields