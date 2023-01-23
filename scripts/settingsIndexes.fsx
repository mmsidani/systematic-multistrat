#r@"..\sa2-dlls\equities.dll"

(*

    What we know as of 11/11/2013 :

        "DAX INDEX" ; no cap – no permission wgts
        "CAC INDEX" ; no cap – no permission wgts
        "UKX INDEX" ; no cap – wgt permission
        "OMX INDEX" ; no cap – no permission wgts
        "SXXP INDEX" ; no cap – no permission wgts
        "TPX INDEX" ; no cap – wgt permission
        "AS51 INDEX" ; no cap – no permission wgts
        "SIMSCI INDEX" ; no cap – wgts permission
        "SPTSX60 INDEX" ; no cap – no permission wgts
        "HSI INDEX" ; 15% cap – wgts permission
        "AEX INDEX" ; no cap – no permission wgts
        "IBEX INDEX" ; no cap – wgts permission
        "FTSEMIB INDEX" ; no cap – no permission
        "OBXP INDEX" ; no cap – wgts permission
        "SMI INDEX" ; no cap – wgts permission
        "TAMSCI INDEX" ; no cap – wgts permission
        "NIFTY INDEX" ; no cap –  no wgts permission
        "SET50 INDEX" ; no cap – wgts permission
        "KOSPI2 INDEX" ; no cap – wgts permission
        "FBMKLCI INDEX" ; no cap – wgts permission
        "WIG20 INDEX" ; cap no more that 5 companies from 1 sector – wgts permission
        "SX5E INDEX" ; no cap – no wgts permission
        "TOP40 INDEX" ; no cap – no wgts permission
        "INDU INDEX" ; no cap – price weighted / equal wgt
        "SPX INDEX" ; no cap – no wgts permission
        "HSCEI INDEX" ; no cap listed but looks like 10% cap – we have wgt permission
        "NKY INDEX" ; no cap – price weighted / equal wgt
        "NDX INDEX" ; 24% cap – no permission
        "RTY INDEX" ; no cap – we have permissions
        "MID INDEX" no cap – no permission

*)


open SA2.Equity.IndexTypes

let indexCaps = 

    {

    cappedIndex = 

        [

        ( "HSI INDEX" , 0.15 ) ;
        ( "NDX INDEX" , 0.24 ) ;

        ]

        |> Map.ofList

    priceWeightedIndex =

        [

        "INDU INDEX" ;
        "NKY INDEX"

        ]

        |> set

    }



let developedIndexes =

    {

        criterion = "future contract with good volume in developed country"

        filteredIndexes =

            [

            "DAX INDEX" ;
            "CAC INDEX" ;
            "UKX INDEX" ;
            "OMX INDEX" ;
            "SXXP INDEX" ;
            "TPX INDEX" ;
            "AS51 INDEX" ;
            "SIMSCI INDEX" ;
            "SPTSX60 INDEX" ;
            "HSI INDEX" ;
            "AEX INDEX" ;
            "IBEX INDEX" ;
            "FTSEMIB INDEX" ;
            "OBXP INDEX" ;
            "SMI INDEX" ;
            "TAMSCI INDEX";
            "SX5E INDEX" ;
            "INDU INDEX" ;
            "NKY INDEX" ;
            "SPX INDEX" ; 
            "NDX INDEX" ;
            "KOSPI2 INDEX" ;
            "RTY INDEX" ;
            "NIFTY INDEX" ;
            "SET50 INDEX" ;
            "FBMKLCI INDEX" ;
            "WIG20 INDEX" ;
            "TOP40 INDEX" ;
            "HSCEI INDEX" ;
            "RTSI$ INDEX" ; 
//            "XIN9I INDEX"; 
            "MEXBOL INDEX"; 
            "IBOV INDEX" ; 
            "XU030 INDEX"; 
//            "SHSN300 INDEX";


            ]

    }



let developingIndexes =

    {

        criterion = "future contract with good volume with the underlying from a developing country "

        filteredIndexes =

            [
            
//            "NIFTY INDEX" ;
//            "SET50 INDEX" ;
//            "FBMKLCI INDEX" ;
//            "WIG20 INDEX" ;
//            "TOP40 INDEX" ;
//            "HSCEI INDEX" ;
//            "RTSI$ INDEX" ; 
//            "XIN9I INDEX"; 
//            "MEXBOL INDEX"; 
//            "IBOV INDEX" ; 
//            "XU030 INDEX"; 
//            "SHSN300 INDEX";

            ]

    }


    
let investableIndexes =

    {

        criterion = "developing and developed indexes with future contract with good volume"

        clusteredIndexes =

            [           
        
                developedIndexes.filteredIndexes ;

                developingIndexes.filteredIndexes 

            ]

    }




let indexUniverse =

    {

        indexUniverse =

            [ 

                developedIndexes.filteredIndexes ;

                developingIndexes.filteredIndexes ;


                // no future contracts

////                [
////
////                    "BUX INDEX" ; 
////                    "KOSPI INDEX" ;
////                    "SHCOMP INDEX" ;
////                    "SZCOMP INDEX" ;
////                    "BGCC200 INDEX" ;
////                    "SBF250 INDEX" ;
////                    "CDAX INDEX" ;
////                    "MCX INDEX" ;
////                    "NZSE50FG INDEX" ;
////                    "IPSA INDEX" ;
////                    "COLCAP INDEX" ;
////                    "JCI INDEX" ;
////                
////                ] ;

                // futures but no volume
        
////                [
////                    "MID INDEX" ;
////                    "MDAX INDEX" ;
////                    "FTASE INDEX" ; 
////                
////                ] ;

                // futures and reasonable volume but we have better in that market

////                [
////                    "TWSE INDEX"
////
////                ]

            ] 

            |> List.concat

    }


