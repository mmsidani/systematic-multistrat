#r@"..\sa2-dlls\marketData.dll"



#load ".\settingsProduction.fsx"

open SA2.MarketData.OecdUpdate



let oecdInput =

    {

    gdpNominalQoqSeriesName = "Gross domestic product - expenditure approach"

    gdpNominalQoqSeriesDescription = "Millions of national currency current prices quarterly levels seasonally adjusted" // Important Note: the description has commas in it in the file that comes from OECD. Make sure they're removed or the splitting logic in the code will break

    gdpNominalQoqSeriesFrequency = "Quarterly"

    gdpQoqFileName = @"\\TERRA\Users\Majed\devel\data\oecd_nominal_gdp_qoq_20131115.csv"

    countryInstrumentMap =

        [ 

        ( "United States" , "us.nomGdp" )  ;
        ( "France" , "fr.nomGdp" )  ;
        ( "Australia" , "au.nomGdp" )  ;
        ( "South Africa" , "za.nomGdp" )  ;
        ( "United Kingdom" , "uk.nomGdp" )  ;
        ( "Korea" , "kr.nomGdp" )  ;
        ( "Norway" , "no.nomGdp" )  ;
        ( "Switzerland" , "ch.nomGdp" )  ;
        ( "Canada" , "ca.nomGdp" )  ;
        ( "Netherlands" , "nl.nomGdp" )  ;
        ( "New Zealand" , "nz.nomGdp" )  ;
        ( "Austria" , "at.nomGdp" )  ;
        ( "Denmark" , "dk.nomGdp" )  ;
        ( "Finland" , "fi.nomGdp" )  ;
        ( "Italy" , "it.nomGdp" )  ;
        ( "Germany" , "de.nomGdp" )  ;
        ( "Sweden" , "se.nomGdp" )  ;
        ( "Argentina" , "ar.nomGdp" )  ;
        ( "Mexico" , "mx.nomGdp" )  ;
        ( "Japan" , "jp.nomGdp" )  ;
        ( "Belgium" , "be.nomGdp" )  ;
        ( "Czech Republic" , "cz.nomGdp" )  ;
        ( "European Union 27" , "eu.nomGdp" )  ; // Important Note: "European Union 27" is  "European Union (27 countries)" in the file downloaded from OECD. Change it first
        ( "Euro area 17" , "ez.nomGdp" )  ; // Important Note: "European Union 17" is  "European Union (17 countries)" in the file downloaded from OECD. Change it first
        ( "Brazil" , "br.nomGdp" )  ;
        ( "Estonia" , "ee.nomGdp" )  ;
        ( "Israel" , "il.nomGdp" )  ;
        ( "Slovenia" , "si.nomGdp" )  ;
        ( "Poland" , "pl.nomGdp" )  ;
        ( "Portugal" , "pt.nomGdp" )  ;
        ( "Slovak Republic" , "sk.nomGdp" )  ;
        ( "Luxembourg" , "lu.nomGdp" )  ;
        ( "Hungary" , "hu.nomGdp" )  ;
        ( "Chile" , "cl.nomGdp" )  ;
        ( "India" , "in.nomGdp" )  ;
        ( "Iceland" , "is.nomGdp" )  ;
        ( "Ireland" , "ie.nomGdp" )  ;
        ( "Turkey" , "tr.nomGdp" )  ;
        ( "Indonesia" , "id.nomGdp" )  ;
        ( "Spain" , "es.nomGdp" )  ;
        ( "Russian Federation" , "ru.nomGdp" )

        ]

        |> Map.ofList

    }

    

// execute

updateGdp oecdInput