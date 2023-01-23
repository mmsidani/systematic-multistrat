module SA2.Common.Dates



open System



let private dayForWeeklyConstant = DayOfWeek.Monday



type DateFrequency =

    | daily = 0
    | weekly = 1
    | monthly = 2
    | yearly = 3
    | quarterly = 4
    | none = 5 // to be used when we don't want to specify it and don't want to introduce options
     



let nextMonth my = 

    let nextMonth = fst my + 1

    if  nextMonth <= 12 then

        ( nextMonth , snd my )

    else

        ( 1 , snd my + 1 )




let date2Obj ( date : int ) = 

    DateTime.ParseExact( date.ToString(), "yyyyMMdd", null )




let obj2Date ( dateObj : DateTime ) =

    let year = dateObj.Year
    let month = dateObj.Month
    let day = dateObj.Day

    year * 10000 + month * 100 + day




let generateDatesSeq startDate timeSpan =

    // Note: Should be used in conjunction with Seq.take n, where n is the number of dates desired

    let dateObj = date2Obj startDate

    let ret = seq { 

        let d = ref dateObj

        while true do

            yield !d
            d := DateTime.op_Addition (!d , timeSpan )

    }

    ret




let thirdFridayOfMonth month year = 

    // return the date of the 3rd Friday of the month and year passed

    let firstDateOfMonth = year * 10000 + month * 100 + 1

    let dates = generateDatesSeq firstDateOfMonth ( TimeSpan( 1 , 0 , 0 , 0 ) )

    // first friday has to fall in first 21 days
    let fridays = dates |> Seq.take( 21 ) |> Seq.filter ( fun d -> d.DayOfWeek = DayOfWeek.Friday ) |> Seq.toArray

    year * 10000 + month * 100 + fridays.[2].Day




let date2dmy date =

    let year = date / 10000
    let month = ( date - year * 10000 ) / 100
    let day = date - year * 10000 - month * 100

    ( 

      ( 
        if day < 10 then

            "0" + day.ToString()

        else

            day.ToString() 

      )

      , 

      ( 

        if month < 10 then

            "0" + month.ToString()

        else

            month.ToString()
      ) 

      ,        

        year.ToString() 

    )




let bbDateTime2ymd ( date : string ) =

    let splits = date.Split( [| 'T' |] ).[ 0 ].Split( [| '-' |] )
    
    ( splits.[ 0 ] , splits.[ 1 ] , splits.[ 2 ] )




let private generateEOMDates dateObjs endDate =

    // to make sure that endDate gets picked if it is an EOM date, we first append one more date

    Seq.append dateObjs [| DateTime.op_Addition ( endDate , TimeSpan ( 1 , 0 , 0 , 0 ) ) |] 

                        |> Seq.pairwise 
                        |> Seq.filter ( fun ( ( d0 , d1 ) : DateTime * DateTime ) -> d0.Month <> d1.Month ) 
                        |> Seq.map ( fun ( d0 , d1 ) -> d0 |> obj2Date )




let datesAtFrequency frequency startDate endDate =

    if endDate < startDate then

        raise ( Exception( "endDate is before startDate" ) )

    let sDateObj = date2Obj startDate
    let eDateObj = date2Obj endDate
    let numberOfDays = 1.0 + DateTime.op_Subtraction( eDateObj , sDateObj ).TotalDays |> int

    let dates = generateDatesSeq startDate ( TimeSpan( 1 , 0 , 0 , 0 ) ) 

                    |> Seq.take( numberOfDays )       
                    |> Seq.filter ( fun d -> d >= sDateObj && d <= eDateObj )

    if frequency = DateFrequency.monthly then

        generateEOMDates dates eDateObj
            
            |> Seq.toArray

    elif frequency = DateFrequency.weekly then

        dates

            |> Seq.filter ( fun ( d : DateTime) -> d.DayOfWeek = dayForWeeklyConstant )

            |> Seq.toArray

            |> Array.map ( fun d -> obj2Date d )

    elif frequency = DateFrequency.daily then

        dates

            |> Seq.filter ( fun ( d : DateTime ) -> d.DayOfWeek <> DayOfWeek.Saturday && d.DayOfWeek <> DayOfWeek.Sunday ) // remove week-end days

            |> Seq.toArray

            |> Array.map ( fun d -> obj2Date d )

    else

        raise ( Exception( "frequency" + ( frequency.ToString() ) + " is not supported yet " ) )




let shiftWeekendsOneDate ( date : DateTime ) =

    match date.DayOfWeek 
                           
        with

            | DayOfWeek.Saturday -> date.AddDays( -1.0 )
            | DayOfWeek.Sunday -> date.AddDays( -2.0 )
            | _ -> date




let shiftWeekends dates =

    // replace Saturdays and Sundays by the previous Fridays

     dates |> List.map ( 
     
                            fun ( d : DateTime ) -> 
    
                                shiftWeekendsOneDate d

                       )



let diffMonths ( date0 : DateTime ) date1 =

    if date1 < date0 then

        raise( Exception "date1 must be larger than date0" )

    ( date1.Year - date0.Year ) * 12 + date1.Month - date0.Month 

    


let map2Quarter ( date : DateTime ) =

    let month = date.Month
    
    if  1 <= month && month <= 3 then 1
    elif 4 <= month && month <= 6 then 2
    elif 7 <= month && month <= 9 then 3
    else 4




let diffQuarters ( date0 : DateTime ) ( date1 : DateTime ) =

    if date1 < date0 then

        raise( Exception "date1 must be larger than date0" )

    ( date1.Year - date0.Year ) * 4 + map2Quarter date1 - map2Quarter date0




let convertStringFrequency freq =

    if freq = "Annual" then

        1.0

    elif freq = "Semiannual" then

        2.0

    elif freq = "Quarterly" then

        4.0

    elif freq = "Monthly" then

        12.0

    elif freq = "Weekly" then

        52.0

    elif freq = "Daily" then

        365.0

    else

        raise( Exception ("frequency " + freq + " is not supported."))




let convertStringTenor ( tenor : string ) =

    let timeUnit = tenor.[ tenor.Length - 1 ].ToString ( )

    if timeUnit = "Y" then

        float tenor.[ 0 .. tenor.Length - 2 ]

    elif timeUnit = "M" then

        float tenor.[ 0 .. tenor.Length - 2 ] / 12.0

    elif timeUnit = "W" then
        
        float tenor.[ 0 .. tenor.Length - 2 ] / 52.0

    elif timeUnit = "D" then

        float tenor.[ 0 .. tenor.Length - 2 ] / 365.0

    elif tenor = "ON" || tenor = "TN" then

        // TODO to be verified

        float tenor.[ 0 .. tenor.Length - 2 ] / 365.0

    else
        
        raise( Exception ("tenor " + tenor + " is not supported."))