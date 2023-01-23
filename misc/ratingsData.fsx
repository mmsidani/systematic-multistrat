#r@"..\sa2-dlls\common.dll"

#load@"..\scripts\settingsRatings.fsx"



open SA2.Common.Io
open SA2.Common.Utils
open SA2.Common.Table_market_data



let readFiles moodysDataFile fitchDataFile snpDataFile =

    let moodysData =

        if moodysDataFile <> "" then
     
            readFile moodysDataFile
                |> Seq.toArray
                |> Array.map ( fun l -> l.Split( [| ',' |] ) )
                |> Array.map ( fun a -> ( a.[ 0 ] |> int , a.[ 1 ] , a.[ 2 ] , a.[ 3 ] ) )
                
        else

            Array.empty

    let fitchData =

        if fitchDataFile <> "" then
     
            readFile fitchDataFile
                |> Seq.toArray
                |> Array.map ( fun l -> l.Split( [| ',' |] ) )
                |> Array.map ( fun a -> ( a.[ 0 ] |> int , a.[ 1 ] , a.[ 2 ] , a.[ 3 ] ) )

        else

            Array.empty

    let snpData =

        if snpDataFile <> "" then
     
            readFile snpDataFile
                |> Seq.toArray
                |> Array.map ( fun l -> l.Split( [| ',' |] ) )
                |> Array.map ( fun a -> ( a.[ 0 ] |> int , a.[ 1 ] , a.[ 2 ] , a.[ 3 ] ) )

        else

            Array.empty

    [ ( "moodys" , moodysData ) ; ( "fitch" , fitchData ) ; ( "snp" , snpData ) ]

        |> Map.ofList




let uploadRatingsData moodysDataFile fitchDataFile snpDataFile =

    let ratingsData = readFiles moodysDataFile fitchDataFile snpDataFile

    let moodysData = 
        let moodysMap = SettingsRatings.ratingsMap.moodys |> Map.ofList
        Map.find "moodys" ratingsData 
            |> Array.map ( fun ( d , c , f , v ) -> printfn "v %s" v ; ( d , c , f , Map.find v moodysMap  ) )

    let fitchData = 
        let fitchMap = SettingsRatings.ratingsMap.fitch |> Map.ofList
        Map.find "fitch" ratingsData 
            |> Array.map ( fun ( d , c , f , v ) -> ( d , c , f , Map.find v fitchMap  ) )

    let snpData = 
        let snpMap = SettingsRatings.ratingsMap.snp |> Map.ofList
        Map.find "snp" ratingsData 
            |> Array.map ( fun ( d , c , f , v ) -> ( d , c , f , Map.find v snpMap  ) )

    let allData = [| moodysData ; fitchData ; snpData |] |> Array.concat
    let allFields = allData |> Array.map ( fun ( _ , _ , f , _ ) -> f ) |> set

    for field in allFields do
        
        allData

        |> Array.filter ( fun ( _ , _ , f , _ ) -> f = field )
        |> Array.map ( fun ( d , c , _ , v ) -> ( d , c , c , float v ) )
        |> (fun junk -> printfn "there %A" junk ; junk )
        |> update_market_data "WEB" field field 1.0 


    
// execute

uploadRatingsData "" "//TERRA/Users/Majed/devel/data/fitch.csv" ""
                        
