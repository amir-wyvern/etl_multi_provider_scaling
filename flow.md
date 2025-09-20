flowchart TD
    
    filter["filter part"] -- provider data --> JsonOut
    filter["filter part"] -- imdb data --> DoesExistIMDb{Does it have IMDb id?}
    filter["filter part"] -- Db data --> RetrieveMetaDataViaTitleDB[Retrieve metadata with Title from DB]
    DoesExistIMDb -- no --> DoesExistTitleEn{Does exist the TitleEn}
    DoesExistTitleEn -- no --> TranslationToEn[Get TitleEn by Translation or AI]
    DoesExistTitleEn -- yes --> SearchByTitleEnIMDbApi[Search by titleEn in IMDb API]
    TranslationToEn --> SearchByTitleEnIMDbApi
    SearchByTitleEnIMDbApi --> GetMostRelevantIMDbApi[Get Most Relevant Search Result]
    GetMostRelevantIMDbApi --> JsonOut
    RetrieveMetaDataViaTitleDB --> GetMostRelevantDB[Get Most Relevant Search Result]
    GetMostRelevantDB --> JsonOut["Get multi results from different sources (IMDb, DB, provider)"]

    JsonOut --> END((End))

    DoesExistIMDb -- yes --> CheckContentType{Check Content Type}
    getContentIMDb --> ExtractMetaData[Extract MetaData]
    ExtractMetaData --> JsonOut

    subgraph ContentIMDbFetching
        CheckContentType -- movie --> getMovieIMDbID[get imdb metadata]
        CheckContentType -- series --> getParentIMDbID[Found parent imdb ID from child imdb id]
        getParentIMDbID --> getParentIMDbMetaData[get parent imdb metadata]
        getParentIMDbMetaData --> getContentIMDb[IMDb & MetaData Output]
        getMovieIMDbID --> getContentIMDb
    end