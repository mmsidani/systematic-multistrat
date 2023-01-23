module SA2.Common.Dictionaries

open System


type StaticDataFieldDictionary =  

    {

        bbStaticField2Sa2Name : Map< string , string >

    }

    member x.mapIt( f ) =

        if Map.containsKey f x.bbStaticField2Sa2Name then

            Map.find f x.bbStaticField2Sa2Name

        else

            raise( Exception ( "StaticDataFieldDictionary does not know key " + f )  )




type DataFieldDictionary =  

    {

        // ( bbFieldName , sa2FieldName , divisor )

        bbField2Sa2NameDivisor : ( string * string * float ) list


        // ( bbInstrumentTicker , sa2FieldName , divisor )
        
        nameFieldDivisorException : ( string * string * float ) list

    }




    member x.mapIt f =

        // sa2Name -> bbFieldName


        // Note: yes, we convert to a map every call, but it's a small structure and we don't call it often anyway. 
        // Alternatives is to change record to a class and create the map at construction time 
        // or change the data structure to Map< string , string * float > which would require something like ( string * ( string * float ) ) list |> Map.ofList in the script file

        let fieldMap = x.bbField2Sa2NameDivisor |> List.map ( fun ( b , s , d ) -> ( s , b ) ) |> Map.ofList

        if Map.containsKey f fieldMap then

             Map.find f fieldMap

        else

            raise( Exception ( "DataFieldDictionary does not have a mapping for key " + f )  )




    member x.divisor f =        

        let fieldMap = x.bbField2Sa2NameDivisor |> List.map ( fun ( b , s , d ) -> ( s , d ) ) |> Map.ofList

        if Map.containsKey f fieldMap then

             Map.find f fieldMap

        else

            raise( Exception ( "DataFieldDictionary does not have a divisor for key " + f )  )


    member x.allDivisorExceptions () =

        (
            x.nameFieldDivisorException |> List.map ( fun ( n , f , _ ) -> ( f , n ) ) |> Map.ofList ,
                
                x.nameFieldDivisorException |> List.map ( fun ( n , _ , d ) -> ( n , d ) ) |> Map.ofList )




type Icb2GicsConverter =

    {

        gicsIcb : ( string * string list) list

    }

    member this.getMapIcb2Gics () =

        this.gicsIcb 

            |> List.unzip

            ||> List.map2 

                ( 
                    fun g is ->

                        is |> List.map ( fun i -> ( i , g ) ) // List.map fine if list is empty 

                )

            |> List.concat

            |> Map.ofList
                        