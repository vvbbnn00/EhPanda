//
//  ReadingView.swift
//  EhPanda
//
//  Created by 荒木辰造 on R 4/01/22.
//

import SwiftUI
import Kingfisher
import SwiftUIPager
import ComposableArchitecture

struct ReadingView: View {
    @Environment(\.colorScheme) private var colorScheme

    let store: Store<ReadingState, ReadingAction>
    @ObservedObject private var viewStore: ViewStore<ReadingState, ReadingAction>
    @Binding private var setting: Setting
    private let blurRadius: Double

    @StateObject private var autoPlayHandler = AutoPlayHandler()
    @StateObject private var gestureHandler = GestureHandler()
    @StateObject private var pageHandler = PageHandler()
    @StateObject private var page: Page = .first()

    init(
        store: Store<ReadingState, ReadingAction>,
        setting: Binding<Setting>, blurRadius: Double
    ) {
        self.store = store
        viewStore = ViewStore(store)
        _setting = setting
        self.blurRadius = blurRadius
    }

    private var backgroundColor: Color {
        colorScheme == .light ? Color(.systemGray4) : Color(.systemGray6)
    }

    var body: some View {
        ZStack {
            backgroundColor.ignoresSafeArea()
            ZStack {
                if setting.readingDirection == .vertical {
                    AdvancedList(
                        page: page, data: viewStore.state.containerDataSource(setting: setting),
                        id: \.self, spacing: setting.contentDividerHeight,
                        gesture: SimultaneousGesture(magnificationGesture, tapGesture),
                        content: imageStack
                    )
                    .disabled(gestureHandler.scale != 1)
                } else {
                    Pager(
                        page: page, data: viewStore.state.containerDataSource(setting: setting),
                        id: \.self, content: imageStack
                    )
                    .horizontal(setting.readingDirection == .rightToLeft ? .rightToLeft : .leftToRight)
                    .swipeInteractionArea(.allAvailable).allowsDragging(gestureHandler.scale == 1)
                }
            }
            .scaleEffect(gestureHandler.scale, anchor: gestureHandler.scaleAnchor)
            .offset(gestureHandler.offset).gesture(tapGesture).gesture(dragGesture)
            .gesture(magnificationGesture).ignoresSafeArea()
            .id(viewStore.databaseLoadingState)
            .id(viewStore.forceRefreshID)
            ControlPanel(
                showsPanel: viewStore.binding(\.$showsPanel),
                showsSliderPreview: viewStore.binding(\.$showsSliderPreview),
                sliderValue: $pageHandler.sliderValue, setting: $setting,
                autoPlayPolicy: .init(get: { autoPlayHandler.policy }, set: setAutoPlayPolocy),
                range: 1...Float(viewStore.gallery.pageCount),
                previewURLs: viewStore.previewURLs,
                dismissGesture: controlPanelDismissGesture,
                dismissAction: { viewStore.send(.onPerformDismiss) },
                navigateSettingAction: { viewStore.send(.setNavigation(.readingSetting)) },
                reloadAllImagesAction: { viewStore.send(.reloadAllWebImages) },
                retryAllFailedImagesAction: { viewStore.send(.retryAllFailedWebImages) },
                fetchPreviewURLsAction: { viewStore.send(.fetchPreviewURLs($0)) }
            )
        }
        .sheet(unwrapping: viewStore.binding(\.$route), case: /ReadingState.Route.readingSetting) { _ in
            NavigationView {
                ReadingSettingView(
                    readingDirection: $setting.readingDirection,
                    prefetchLimit: $setting.prefetchLimit,
                    enablesLandscape: $setting.enablesLandscape,
                    contentDividerHeight: $setting.contentDividerHeight,
                    maximumScaleFactor: $setting.maximumScaleFactor,
                    doubleTapScaleFactor: $setting.doubleTapScaleFactor
                )
                .toolbar {
                    CustomToolbarItem(placement: .cancellationAction) {
                        if !DeviceUtil.isPad && DeviceUtil.isLandscape {
                            Button {
                                viewStore.send(.setNavigation(nil))
                            } label: {
                                Image(systemSymbol: .chevronDown)
                            }
                        }
                    }
                }
            }
            .accentColor(setting.accentColor).tint(setting.accentColor)
            .autoBlur(radius: blurRadius).navigationViewStyle(.stack)
        }
        .sheet(unwrapping: viewStore.binding(\.$route), case: /ReadingState.Route.share) { route in
            ActivityView(activityItems: [route.wrappedValue])
                .accentColor(setting.accentColor)
                .autoBlur(radius: blurRadius)
        }
        .progressHUD(
            config: viewStore.hudConfig,
            unwrapping: viewStore.binding(\.$route),
            case: /ReadingState.Route.hud
        )

        // Page
        .onChange(of: page.index) { pageIndex in
            Logger.info("page.index changed", context: ["pageIndex": pageIndex])
            let newValue = pageHandler.mapFromPager(
                index: pageIndex, pageCount: viewStore.gallery.pageCount, setting: setting
            )
            pageHandler.sliderValue = .init(newValue)
            if pageIndex != 0 {
                viewStore.send(.syncReadingProgress(.init(newValue)))
            }
        }
        .onChange(of: pageHandler.sliderValue) { sliderValue in
            Logger.info("pageHandler.sliderValue changed", context: ["sliderValue": sliderValue])
            let newValue = pageHandler.mapToPager(
                index: .init(sliderValue), setting: setting
            )
            if page.index != newValue {
                page.update(.new(index: newValue))
                Logger.info("Pager.update", context: ["update": newValue])
            }
        }
        .onChange(of: viewStore.readingProgress) { readingProgress in
            Logger.info("viewStore.readingProgress changed", context: ["readingProgress": readingProgress])
            pageHandler.sliderValue = .init(readingProgress)
        }

        // AutoPlay
        .onChange(of: viewStore.route) { route in
            Logger.info("viewStore.route changed", context: ["route": route])
            if ![.hud, .none].contains(route) {
                setAutoPlayPolocy(.off)
            }
        }
        .onChange(of: viewStore.showsSliderPreview) { newValue in
            Logger.info("viewStore.showsSliderPreview changed", context: ["newValue": newValue])
            setAutoPlayPolocy(.off)
        }

        // Orientation
        .onChange(of: setting.enablesLandscape) { newValue in
            Logger.info("setting.enablesLandscape changed", context: ["newValue": newValue])
            viewStore.send(.setOrientationPortrait(!newValue))
        }

        .animation(.linear(duration: 0.1), value: gestureHandler.offset)
        .animation(.default, value: gestureHandler.scale)
        .animation(.default, value: viewStore.showsPanel)
        .statusBar(hidden: !viewStore.showsPanel)
        .onDisappear { setAutoPlayPolocy(.off) }
        .onAppear { viewStore.send(.onAppear(setting.enablesLandscape)) }
    }

    @ViewBuilder private func imageStack(index: Int) -> some View {
        let imageStackConfig = viewStore.state.imageContainerConfigs(index: index, setting: setting)
        let isDualPage = setting.enablesDualPageMode && setting.readingDirection != .vertical && DeviceUtil.isLandscape
        HorizontalImageStack(
            index: index, isDualPage: isDualPage, isDatabaseLoading: viewStore.databaseLoadingState != .idle,
            backgroundColor: backgroundColor, config: imageStackConfig, imageURLs: viewStore.imageURLs,
            originalImageURLs: viewStore.originalImageURLs, loadingStates: viewStore.imageURLLoadingStates,
            fetchAction: { viewStore.send(.fetchImageURLs($0)) },
            refetchAction: { viewStore.send(.refetchImageURLs($0)) },
            prefetchAction: { viewStore.send(.prefetchImages($0, setting.prefetchLimit)) },
            loadRetryAction: { viewStore.send(.onWebImageRetry($0)) },
            loadSucceededAction: { viewStore.send(.onWebImageSucceeded($0)) },
            loadFailedAction: { viewStore.send(.onWebImageFailed($0)) },
            copyImageAction: { viewStore.send(.copyImage($0)) },
            saveImageAction: { viewStore.send(.saveImage($0)) },
            shareImageAction: { viewStore.send(.shareImage($0)) }
        )
    }
}

// MARK: AutoPlay
extension ReadingView {
    func setAutoPlayPolocy(_ policy: AutoPlayPolicy) {
        autoPlayHandler.setPolicy(policy, updatePageAction: {
            page.update(.next)
            Logger.info("Pager.update", context: ["update": "next"])
        })
    }
}

// MARK: Gesture
extension ReadingView {
    var tapGesture: some Gesture {
        let singleTap = TapGesture(count: 1)
            .onEnded {
                gestureHandler.onSingleTapGestureEnded(
                    readingDirection: setting.readingDirection,
                    setPageIndexOffsetAction: {
                        let newValue = page.index + $0
                        page.update(.new(index: newValue))
                        Logger.info("Pager.update", context: ["update": newValue])
                    },
                    toggleShowsPanelAction: { viewStore.send(.toggleShowsPanel) }
                )
            }
        let doubleTap = TapGesture(count: 2)
            .onEnded {
                gestureHandler.onDoubleTapGestureEnded(
                    scaleMaximum: setting.maximumScaleFactor,
                    doubleTapScale: setting.doubleTapScaleFactor
                )
            }
        return ExclusiveGesture(doubleTap, singleTap)
    }
    var magnificationGesture: some Gesture {
        MagnificationGesture()
            .onChanged {
                gestureHandler.onMagnificationGestureChanged(
                    value: $0, scaleMaximum: setting.maximumScaleFactor
                )
            }
            .onEnded {
                gestureHandler.onMagnificationGestureEnded(
                    value: $0, scaleMaximum: setting.maximumScaleFactor
                )
            }
    }
    var dragGesture: some Gesture {
        DragGesture(minimumDistance: .zero, coordinateSpace: .local)
            .onChanged(gestureHandler.onDragGestureChanged)
            .onEnded(gestureHandler.onDragGestureEnded)
    }
    var controlPanelDismissGesture: some Gesture {
        DragGesture().onEnded {
            gestureHandler.onControlPanelDismissGestureEnded(
                value: $0, dismissAction: { viewStore.send(.onPerformDismiss) }
            )
        }
    }
}

// MARK: HorizontalImageStack
private struct HorizontalImageStack: View {
    private let index: Int
    private let isDualPage: Bool
    private let isDatabaseLoading: Bool
    private let backgroundColor: Color
    private let config: ImageStackConfig
    private let imageURLs: [Int: URL]
    private let originalImageURLs: [Int: URL]
    private let loadingStates: [Int: LoadingState]
    private let fetchAction: (Int) -> Void
    private let refetchAction: (Int) -> Void
    private let prefetchAction: (Int) -> Void
    private let loadRetryAction: (Int) -> Void
    private let loadSucceededAction: (Int) -> Void
    private let loadFailedAction: (Int) -> Void
    private let copyImageAction: (URL) -> Void
    private let saveImageAction: (URL) -> Void
    private let shareImageAction: (URL) -> Void

    init(
        index: Int, isDualPage: Bool, isDatabaseLoading: Bool, backgroundColor: Color,
        config: ImageStackConfig, imageURLs: [Int: URL], originalImageURLs: [Int: URL],
        loadingStates: [Int: LoadingState], fetchAction: @escaping (Int) -> Void,
        refetchAction: @escaping (Int) -> Void, prefetchAction: @escaping (Int) -> Void,
        loadRetryAction: @escaping (Int) -> Void, loadSucceededAction: @escaping (Int) -> Void,
        loadFailedAction: @escaping (Int) -> Void, copyImageAction: @escaping (URL) -> Void,
        saveImageAction: @escaping (URL) -> Void, shareImageAction: @escaping (URL) -> Void
    ) {
        self.index = index
        self.isDualPage = isDualPage
        self.isDatabaseLoading = isDatabaseLoading
        self.backgroundColor = backgroundColor
        self.config = config
        self.imageURLs = imageURLs
        self.originalImageURLs = originalImageURLs
        self.loadingStates = loadingStates
        self.fetchAction = fetchAction
        self.refetchAction = refetchAction
        self.prefetchAction = prefetchAction
        self.loadRetryAction = loadRetryAction
        self.loadSucceededAction = loadSucceededAction
        self.loadFailedAction = loadFailedAction
        self.copyImageAction = copyImageAction
        self.saveImageAction = saveImageAction
        self.shareImageAction = shareImageAction
    }

    var body: some View {
        HStack(spacing: 0) {
            if config.isFirstAvailable {
                imageContainer(index: config.firstIndex)
            }
            if config.isSecondAvailable {
                imageContainer(index: config.secondIndex)
            }
        }
    }

    func imageContainer(index: Int) -> some View {
        ImageContainer(
            index: index,
            imageURL: imageURLs[index],
            loadingState: loadingStates[index] ?? .idle,
            isDualPage: isDualPage,
            backgroundColor: backgroundColor,
            refetchAction: refetchAction,
            loadRetryAction: loadRetryAction,
            loadSucceededAction: loadSucceededAction,
            loadFailedAction: loadFailedAction
        )
        .onAppear {
            if !isDatabaseLoading {
                if imageURLs[index] == nil {
                    fetchAction(index)
                }
                prefetchAction(index)
            }
        }
        .contextMenu { contextMenuItems(index: index) }
    }
    @ViewBuilder private func contextMenuItems(index: Int) -> some View {
        Button {
            refetchAction(index)
        } label: {
            Label(R.string.localizable.readingViewContextMenuButtonReload(), systemSymbol: .arrowCounterclockwise)
        }
        if let imageURL = imageURLs[index] {
            Button {
                copyImageAction(imageURL)
            } label: {
                Label(R.string.localizable.readingViewContextMenuButtonCopy(), systemSymbol: .plusSquareOnSquare)
            }
            Button {
                saveImageAction(imageURL)
            } label: {
                Label(R.string.localizable.readingViewContextMenuButtonSave(), systemSymbol: .squareAndArrowDown)
            }
            if let originalImageURL = originalImageURLs[index] {
                Button {
                    saveImageAction(originalImageURL)
                } label: {
                    Label(
                        R.string.localizable.readingViewContextMenuButtonSaveOriginal(),
                        systemSymbol: .squareAndArrowDownOnSquare
                    )
                }
            }
            Button {
                shareImageAction(imageURL)
            } label: {
                Label(R.string.localizable.readingViewContextMenuButtonShare(), systemSymbol: .squareAndArrowUp)
            }
        }
    }
}

// MARK: ImageContainer
private struct ImageContainer: View {
    private var width: CGFloat {
        DeviceUtil.windowW / (isDualPage ? 2 : 1)
    }
    private var height: CGFloat {
        width / Defaults.ImageSize.contentAspect
    }

    private let index: Int
    private let imageURL: URL?
    private let loadingState: LoadingState
    private let isDualPage: Bool
    private let backgroundColor: Color
    private let refetchAction: (Int) -> Void
    private let loadRetryAction: (Int) -> Void
    private let loadSucceededAction: (Int) -> Void
    private let loadFailedAction: (Int) -> Void

    init(
        index: Int, imageURL: URL?,
        loadingState: LoadingState,
        isDualPage: Bool, backgroundColor: Color,
        refetchAction: @escaping (Int) -> Void,
        loadRetryAction: @escaping (Int) -> Void,
        loadSucceededAction: @escaping (Int) -> Void,
        loadFailedAction: @escaping (Int) -> Void
    ) {
        self.index = index
        self.imageURL = imageURL
        self.loadingState = loadingState
        self.isDualPage = isDualPage
        self.backgroundColor = backgroundColor
        self.refetchAction = refetchAction
        self.loadRetryAction = loadRetryAction
        self.loadSucceededAction = loadSucceededAction
        self.loadFailedAction = loadFailedAction
    }

    private func placeholder(_ progress: Progress) -> some View {
        Placeholder(style: .progress(
            pageNumber: index, progress: progress,
            isDualPage: isDualPage, backgroundColor: backgroundColor
        ))
        .frame(width: width, height: height)
    }
    @ViewBuilder private func image(url: URL?) -> some View {
        if url?.absoluteString.contains(".gif") != true {
            KFImage(url)
                .placeholder(placeholder)
                .defaultModifier(withRoundedCorners: false)
                .onSuccess(onSuccess).onFailure(onFailure)
        } else {
            KFAnimatedImage(url)
                .placeholder(placeholder).fade(duration: 0.25)
                .onSuccess(onSuccess).onFailure(onFailure)
        }
    }

    var body: some View {
        if loadingState == .idle {
            image(url: imageURL).scaledToFit()
        } else {
            ZStack {
                backgroundColor
                VStack {
                    Text(String(index)).font(.largeTitle.bold())
                        .foregroundColor(.gray).padding(.bottom, 30)
                    ZStack {
                        Button(action: reloadImage) {
                            Image(systemSymbol: .exclamationmarkArrowTriangle2Circlepath)
                        }
                        .font(.system(size: 30, weight: .medium)).foregroundColor(.gray)
                        .opacity(loadingState == .loading ? 0 : 1)
                        ProgressView().opacity(loadingState == .loading ? 1 : 0)
                    }
                }
            }
            .frame(width: width, height: height)
        }
    }
    private func reloadImage() {
        if let error = (/LoadingState.failed).extract(from: loadingState) {
            if case .webImageFailed = error {
                loadRetryAction(index)
            } else {
                refetchAction(index)
            }
        }
    }
    private func onSuccess(_: RetrieveImageResult) {
        loadSucceededAction(index)
    }
    private func onFailure(_: KingfisherError) {
        if imageURL != nil {
            loadFailedAction(index)
        }
    }
}

// MARK: Definition
struct ImageStackConfig {
    let firstIndex: Int
    let secondIndex: Int
    let isFirstAvailable: Bool
    let isSecondAvailable: Bool
}

enum AutoPlayPolicy: Int, CaseIterable, Identifiable {
    var id: Int { rawValue }

    case off = -1
    case sec1 = 1
    case sec2 = 2
    case sec3 = 3
    case sec4 = 4
    case sec5 = 5
}

extension AutoPlayPolicy {
    var value: String {
        switch self {
        case .off:
            return R.string.localizable.enumAutoPlayPolicyValueOff()
        default:
            return R.string.localizable.commonValueSeconds("\(rawValue)")
        }
    }
}

struct ReadingView_Previews: PreviewProvider {
    static var previews: some View {
        NavigationView {
            Text("")
                .fullScreenCover(isPresented: .constant(true)) {
                    ReadingView(
                        store: .init(
                            initialState: .init(gallery: .empty),
                            reducer: readingReducer,
                            environment: ReadingEnvironment(
                                urlClient: .live,
                                imageClient: .live,
                                deviceClient: .live,
                                hapticClient: .live,
                                cookiesClient: .live,
                                databaseClient: .live,
                                clipboardClient: .live,
                                appDelegateClient: .live
                            )
                        ),
                        setting: .constant(.init()),
                        blurRadius: 0
                    )
                }
        }
    }
}
