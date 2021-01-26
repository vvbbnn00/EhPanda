//
//  AppState.swift
//  EhPanda
//
//  Created by 荒木辰造 on R 2/12/26.
//

import Foundation

struct AppState {
    var environment = Environment()
    var settings = Settings()
    var homeInfo = HomeInfo()
    var detailInfo = DetailInfo()
    var commentInfo = CommentInfo()
    var contentInfo = ContentInfo()
    var cachedList = CachedList()
}

extension AppState {
    struct Environment {
        var slideMenuOffset = -Defaults.FrameSize.slideMenuWidth
        var navBarHidden = false
        var homeListType: HomeListType = .frontpage
        var homeViewSheetState: HomeViewSheetState? = nil
        var settingViewSheetState: SettingViewSheetState? = nil
        var settingViewActionSheetState: SettingViewActionSheetState? = nil
        var filterViewActionSheetState: FilterViewActionSheetState? = nil
        var detailViewSheetState: DetailViewSheetState? = nil
        var commentViewSheetState: CommentViewSheetState? = nil
    }
    
    struct Settings {
        var userInfoLoading = false
        
        @FileStorage(directory: .cachesDirectory, fileName: "user.json")
        var user: User?
        @FileStorage(directory: .cachesDirectory, fileName: "filter.json")
        var filter: Filter?
        @FileStorage(directory: .cachesDirectory, fileName: "setting.json")
        var setting: Setting?
    }
}

extension AppState {
    struct HomeInfo {
        var searchKeyword = ""
        
        var searchItems: [Manga]?
        var searchLoading = false
        var searchNotFound = false
        var searchLoadFailed = false
        var searchCurrentPageNum = 0
        var searchPageNumMaximum = 1
        var moreSearchLoading = false
        
        var frontpageItems: [Manga]?
        var frontpageLoading = false
        var frontpageNotFound = false
        var frontpageLoadFailed = false
        var frontpageCurrentPageNum = 0
        var frontpagePageNumMaximum = 1
        var moreFrontpageLoading = false
        
        var popularItems: [Manga]?
        var popularLoading = false
        var popularNotFound = false
        var popularLoadFailed = false
        
        var watchedItems: [Manga]?
        var watchedLoading = false
        var watchedNotFound = false
        var watchedLoadFailed = false
        var watchedCurrentPageNum = 0
        var watchedPageNumMaximum = 1
        var moreWatchedLoading = false
        
        var favoritesItems: [Manga]?
        var favoritesLoading = false
        var favoritesNotFound = false
        var favoritesLoadFailed = false
        var favoritesCurrentPageNum = 0
        var favoritesPageNumMaximum = 1
        var moreFavoritesLoading = false
        
        @FileStorage(directory: .cachesDirectory, fileName: "historyList.json")
        var historyItems: [String : Manga]?
        
        func isFavored(id: String) -> Bool {
            if let items = favoritesItems {
                let filteredItems = items
                    .filter { $0.id == id }
                return !filteredItems.isEmpty
            } else {
                return false
            }
        }
        mutating func insertSearchItems(mangas: [Manga]) {
            mangas.forEach { manga in
                if searchItems?.contains(manga) == false {
                    searchItems?.append(manga)
                }
            }
        }
        mutating func insertFrontpageItems(mangas: [Manga]) {
            mangas.forEach { manga in
                if frontpageItems?.contains(manga) == false {
                    frontpageItems?.append(manga)
                }
            }
        }
        mutating func insertWatchedItems(mangas: [Manga]) {
            mangas.forEach { manga in
                if watchedItems?.contains(manga) == false {
                    watchedItems?.append(manga)
                }
            }
        }
        mutating func insertFavoritesItems(mangas: [Manga]) {
            mangas.forEach { manga in
                if favoritesItems?.contains(manga) == false {
                    favoritesItems?.append(manga)
                }
            }
        }
        mutating func insertHistoryItem(manga: Manga?) {
            guard var manga = manga else { return }
            if historyItems != nil {
                if historyItems?.keys.contains(manga.id) == true {
                    historyItems?[manga.id]?.lastOpenTime = Date()
                } else {
                    manga.lastOpenTime = Date()
                    historyItems?[manga.id] = manga
                }
            } else {
                historyItems = Dictionary(
                    uniqueKeysWithValues: [(manga.id, manga)]
                )
            }
        }
    }
    
    struct DetailInfo {
        var commentContent = ""
        
        var mangaDetailLoading = false
        var mangaDetailLoadFailed = false
        
        var associatedItems: [AssociatedItem] = []
        var associatedItemsLoading = false
        var associatedItemsNotFound = false
        var associatedItemsLoadFailed = false
        var moreAssociatedItemsLoading = false
        
        var alterImagesLoading = false
        
        var mangaCommentsUpdating = false
        var mangaCommentsUpdateFailed = false
        
        mutating func removeAssociatedItems(depth: Int) {
            if associatedItems.count >= depth + 1 {
                associatedItems[depth].mangas = []
            }
        }
        
        mutating func replaceAssociatedItems(
            depth: Int,
            keyword: AssociatedKeyword,
            pageNum: PageNumber,
            items: [Manga]
        ) {
            if associatedItems.count >= depth + 1 {
                associatedItems[depth] = AssociatedItem(
                    keyword: keyword,
                    pageNum: pageNum,
                    mangas: items
                )
            } else {
                associatedItems
                    .append(AssociatedItem(
                        keyword: keyword,
                        pageNum: pageNum,
                        mangas: items
                ))
            }
        }
        
        mutating func insertAssociatedItems(
            depth: Int,
            keyword: AssociatedKeyword,
            pageNum: PageNumber,
            items: [Manga]
        ) {
            if associatedItems.count >= depth + 1 {
                associatedItems[depth].keyword = keyword
                associatedItems[depth].pageNum = pageNum
                associatedItems[depth].mangas.append(contentsOf: items)
            } else {
                print("関連リスト更新: 元の序列が見つかりませんでした")
            }
        }
    }
    
    struct CommentInfo {
        var commentContent = ""
    }
    
    struct ContentInfo {
        var mangaContentsLoading = false
        var mangaContentsLoadFailed = false
    }
}

extension AppState {
    struct CachedList {
        @FileStorage(directory: .cachesDirectory, fileName: "cachedList.json")
        var items: [String : Manga]?
        
        mutating func cache(mangas: [Manga]) {
            let previousCount = items?.count ?? 0
            if items == nil {
                items = Dictionary(uniqueKeysWithValues: mangas.map { ($0.id, $0) })
                return
            }
            
            for manga in mangas {
                if items?[manga.id] == nil {
                    items?[manga.id] = manga
                }
            }
            let currentCount = items?.count ?? 0
            print("キャッシュ済みリスト 更新: \(previousCount) -> \(currentCount)")
        }
        
        mutating func insertDetail(id: String, detail: MangaDetail) {
            items?[id]?.detail = detail
        }
        mutating func insertAlterImages(id: String, images: [MangaAlterData]) {
            items?[id]?.detail?.alterImages = images
        }
        mutating func updateComments(id: String, comments: [MangaComment]) {
            items?[id]?.detail?.comments = comments
        }
        mutating func insertContents(id: String, contents: [MangaContent]) {
            if items?[id]?.contents == nil {
                items?[id]?.contents = contents.sorted { $0.tag < $1.tag }
            } else {
                contents.forEach { content in
                    if items?[id]?.contents?.contains(content) == false {
                        items?[id]?.contents?.append(content)
                    }
                }
                items?[id]?.contents?.sort { $0.tag < $1.tag }
            }
        }
        mutating func insertReadingProgress(id: String, progress: Int) {
            items?[id]?.detail?.readingProgress = progress
        }
    }
}
