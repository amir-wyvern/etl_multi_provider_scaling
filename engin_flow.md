flowchart TD
    Start((start)) --> retrieve["previos block"]
    retrieve --> checkExistDbRecord{Does exist key db for that provider key}
    checkExistDbRecord -- yes --> useDbRecord[use the db imdb id for that content]

    checkExistDbRecord -- no --> checkOtherSources{check other source like provider and IMDb api for existing imdb id field}

    checkOtherSources -- provider have the imdb_id field and IMDb api have result for that imdb_id --> useProviderIMDbID[use the provider imdb id]
    checkOtherSources -- provider have not imdb_id and IMDb api have result for that title --> useIMDbApiImdbID[use the imdb id from IMDb api]
    checkOtherSources -- provider have not imdb_id and have no result for that title in IMDb api --> setNan[set Nan for imdb id ]
    checkOtherSources -- provider have the imdb_id field and IMDb api have no result for imdb_id --> wrongCons[this condition in this block is wrong and must not happened]

    useProviderIMDbID --> setIMDbID[set imdb id for this content]
    useIMDbApiImdbID --> setIMDbID
    useDbRecord --> setIMDbID
    setIMDbID --> End((end))
    setNan --> End
    wrongCons --> raiseError[raise error for this scenario]
    raiseError --> End