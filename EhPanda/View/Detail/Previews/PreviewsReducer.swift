//
//  PreviewsReducer.swift
//  EhPanda
//
//  Created by 荒木辰造 on R 4/01/16.
//

import Foundation
import ComposableArchitecture

@Reducer
struct PreviewsReducer {
    @CasePathable
    enum Route: Equatable {
        case reading(EquatableVoid = .init())
    }

    // MARK: - CancelID
    // Originally it was CaseIterable, but now it's changed to Hashable
    // so that each ongoing request can have a unique cancel ID.
    private enum CancelID: Hashable {
        case fetchDatabaseInfos
        case fetchPreviewURLs(Int)
    }

    @ObservableState
    struct State: Equatable {
        var route: Route?

        var gallery: Gallery = .empty
        var loadingState: LoadingState = .idle
        var databaseLoadingState: LoadingState = .loading

        var previewURLs = [Int: URL]()
        var previewConfig: PreviewConfig = .normal(rows: 4)

        var readingState = ReadingReducer.State()

        // MARK: - Concurrency-limiting fields
        // requestQueue: The queue for indices waiting to be requested.
        // ongoingRequests: A set of indices that are currently being requested.
        var requestQueue: [Int] = []
        var ongoingRequests: Set<Int> = []

        mutating func updatePreviewURLs(_ previewURLs: [Int: URL]) {
            self.previewURLs = self.previewURLs.merging(
                previewURLs, uniquingKeysWith: { stored, _ in stored }
            )
        }
    }

    enum Action: BindableAction {
        case binding(BindingAction<State>)
        case setNavigation(Route?)
        case clearSubStates

        case syncPreviewURLs([Int: URL])
        case updateReadingProgress(Int)

        case teardown
        case fetchDatabaseInfos(String)
        case fetchDatabaseInfosDone(GalleryState)

        // MARK: - Split the fetchPreviewURLs flow:
        // 1) .fetchPreviewURLs(Int) only enqueues the index
        // 2) .processQueue checks if we can process more requests (max concurrency = 3)
        // 3) .fetchPreviewURLsResponse(index: Int, result: ...) handles the request result
        case fetchPreviewURLs(Int)
        case processQueue
        case fetchPreviewURLsResponse(index: Int, Result<[Int: URL], AppError>)

        case reading(ReadingReducer.Action)
    }

    @Dependency(\.databaseClient) private var databaseClient
    @Dependency(\.hapticsClient) private var hapticsClient

    var body: some Reducer<State, Action> {
        BindingReducer()
            .onChange(of: \.route) { _, newValue in
                Reduce({ _, _ in newValue == nil ? .send(.clearSubStates) : .none })
            }

        Reduce { state, action in
            switch action {
            // MARK: - Binding
            case .binding:
                return .none

            // MARK: - Navigation
            case .setNavigation(let route):
                state.route = route
                return route == nil ? .send(.clearSubStates) : .none

            case .clearSubStates:
                state.readingState = .init()
                return .send(.reading(.teardown))

            // MARK: - Database sync
            case .syncPreviewURLs(let previewURLs):
                return .run { [state] _ in
                    await databaseClient.updatePreviewURLs(gid: state.gallery.id, previewURLs: previewURLs)
                }

            case .updateReadingProgress(let progress):
                return .run { [state] _ in
                    await databaseClient.updateReadingProgress(gid: state.gallery.id, progress: progress)
                }

            // MARK: - Teardown
            // Cancel all ongoing fetchPreviewURLs requests and fetchDatabaseInfos
            case .teardown:
                return .merge(
                    // Cancel ongoing requests individually
                    state.ongoingRequests.map {
                        Effect.cancel(id: CancelID.fetchPreviewURLs($0))
                    }
                    +
                    // Also cancel the fetchDatabaseInfos request if needed
                    [.cancel(id: CancelID.fetchDatabaseInfos)]
                )

            // MARK: - Fetch database info
            case .fetchDatabaseInfos(let gid):
                guard let gallery = databaseClient.fetchGallery(gid: gid) else { return .none }
                state.gallery = gallery
                return .run { [state] send in
                    guard let dbState = await databaseClient.fetchGalleryState(gid: state.gallery.id) else { return }
                    await send(.fetchDatabaseInfosDone(dbState))
                }
                .cancellable(id: CancelID.fetchDatabaseInfos)

            case .fetchDatabaseInfosDone(let galleryState):
                if let previewConfig = galleryState.previewConfig {
                    state.previewConfig = previewConfig
                }
                state.previewURLs = galleryState.previewURLs
                state.databaseLoadingState = .idle
                return .none

            // MARK: - Concurrency-limited fetchPreviewURLs
            case .fetchPreviewURLs(let index):
                // If this index is already being requested, do nothing.
                guard !state.ongoingRequests.contains(index) else {
                    return .none
                }
                // Otherwise, enqueue the index.
                state.requestQueue.append(index)
                // Trigger queue processing
                return .send(.processQueue)

            case .processQueue:
                var effects: [Effect<Action>] = []
                // Process the queue while ongoing requests are fewer than 3
                while state.ongoingRequests.count < 3, !state.requestQueue.isEmpty {
                    let index = state.requestQueue.removeFirst()
                    state.ongoingRequests.insert(index)

                    guard let galleryURL = state.gallery.galleryURL else {
                        // If no valid URL, remove from ongoingRequests immediately.
                        state.ongoingRequests.remove(index)
                        continue
                    }
                    state.loadingState = .loading
                    let pageNum = state.previewConfig.pageNumber(index: index)

                    // Actually perform the request
                    let effect: Effect<Action> = .run { send in
                        let response = await GalleryPreviewURLsRequest(
                            galleryURL: galleryURL,
                            pageNum: pageNum
                        ).response()
                        await send(.fetchPreviewURLsResponse(index: index, response))
                    }
                    .cancellable(id: CancelID.fetchPreviewURLs(index))

                    effects.append(effect)
                }
                return .merge(effects)

            case .fetchPreviewURLsResponse(let index, let result):
                // Remove the index from ongoing requests
                state.ongoingRequests.remove(index)
                state.loadingState = .idle

                switch result {
                case .success(let previewURLs):
                    guard !previewURLs.isEmpty else {
                        state.loadingState = .failed(.notFound)
                        return .none
                    }
                    state.updatePreviewURLs(previewURLs)
                    // Send sync to database, then attempt to process next in queue
                    return .merge(
                        .send(.syncPreviewURLs(previewURLs)),
                        .send(.processQueue)
                    )
                case .failure(let error):
                    state.loadingState = .failed(error)
                    // Attempt to process the next index in the queue
                    return .send(.processQueue)
                }

            // MARK: - Reading
            case .reading(.onPerformDismiss):
                return .send(.setNavigation(nil))

            case .reading:
                return .none
            }
        }
        .haptics(
            unwrapping: \.route,
            case: \.reading,
            hapticsClient: hapticsClient
        )
        // Scope for reading reducer
        Scope(state: \.readingState, action: \.reading, child: ReadingReducer.init)
    }
}
