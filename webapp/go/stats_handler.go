package main

import (
	"database/sql"
	"errors"
	"net/http"
	"sort"
	"strconv"

	"github.com/jmoiron/sqlx"
	"github.com/labstack/echo/v4"
)

type LivestreamStatistics struct {
	Rank           int64 `json:"rank"`
	ViewersCount   int64 `json:"viewers_count"`
	TotalReactions int64 `json:"total_reactions"`
	TotalReports   int64 `json:"total_reports"`
	MaxTip         int64 `json:"max_tip"`
}

type LivestreamRankingEntry struct {
	LivestreamID int64
	Score        int64
}
type LivestreamRanking []LivestreamRankingEntry

func (r LivestreamRanking) Len() int      { return len(r) }
func (r LivestreamRanking) Swap(i, j int) { r[i], r[j] = r[j], r[i] }
func (r LivestreamRanking) Less(i, j int) bool {
	if r[i].Score == r[j].Score {
		return r[i].LivestreamID < r[j].LivestreamID
	} else {
		return r[i].Score < r[j].Score
	}
}

type UserStatistics struct {
	Rank              int64  `json:"rank"`
	ViewersCount      int64  `json:"viewers_count"`
	TotalReactions    int64  `json:"total_reactions"`
	TotalLivecomments int64  `json:"total_livecomments"`
	TotalTip          int64  `json:"total_tip"`
	FavoriteEmoji     string `json:"favorite_emoji"`
}

type UserRankingEntry struct {
	Username string
	Score    int64
}
type UserRanking []UserRankingEntry

func (r UserRanking) Len() int      { return len(r) }
func (r UserRanking) Swap(i, j int) { r[i], r[j] = r[j], r[i] }
func (r UserRanking) Less(i, j int) bool {
	if r[i].Score == r[j].Score {
		return r[i].Username < r[j].Username
	} else {
		return r[i].Score < r[j].Score
	}
}

func getUserStatisticsHandler(c echo.Context) error {
	ctx := c.Request().Context()

	if err := verifyUserSession(c); err != nil {
		// echo.NewHTTPErrorが返っているのでそのまま出力
		return err
	}

	username := c.Param("username")
	// ユーザごとに、紐づく配信について、累計リアクション数、累計ライブコメント数、累計売上金額を算出
	// また、現在の合計視聴者数もだす

	tx, err := dbConn.Connx(ctx)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to get connection: "+err.Error())
	}
	defer tx.Close()

	var user UserModel
	if err := tx.GetContext(ctx, &user, "SELECT * FROM users WHERE name = ?", username); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return echo.NewHTTPError(http.StatusBadRequest, "not found user that has the given username")
		} else {
			return echo.NewHTTPError(http.StatusInternalServerError, "failed to get user: "+err.Error())
		}
	}

	// ランク算出
	var users []*UserModel
	if err := tx.SelectContext(ctx, &users, "SELECT * FROM users"); err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to get users: "+err.Error())
	}

	// ランク算出
	var ranking UserRanking

	// usersのIDリストを抽出
	userIDs := make([]int64, 0, len(users))
	userNames := make(map[int64]string) // ユーザーIDと名前のマッピング
	for _, user := range users {
		userIDs = append(userIDs, user.ID)
		userNames[user.ID] = user.Name
	}

	// reactionsを一度に取得（JOINなし）
	reactionsMap := make(map[int64]int64)
	queryReactions := `
    SELECT l.user_id, COUNT(r.id) as reactions_count
    FROM reactions r
    INNER JOIN livestreams l ON r.livestream_id = l.id
    WHERE l.user_id IN (?)
    GROUP BY l.user_id
`
	query, args, err := sqlx.In(queryReactions, userIDs)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to build reactions query: "+err.Error())
	}
	query = tx.Rebind(query)
	rows, err := tx.QueryxContext(ctx, query, args...)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to fetch reactions: "+err.Error())
	}
	defer rows.Close()

	for rows.Next() {
		var userID, reactions int64
		if err := rows.Scan(&userID, &reactions); err != nil {
			return echo.NewHTTPError(http.StatusInternalServerError, "failed to scan reactions: "+err.Error())
		}
		reactionsMap[userID] = reactions
	}

	// tipsを一度に取得（JOINなし）
	tipsMap := make(map[int64]int64)
	queryTips := `
    SELECT l.user_id, IFNULL(SUM(lc.tip), 0) as total_tips
    FROM livecomments lc
    INNER JOIN livestreams l ON lc.livestream_id = l.id
    WHERE l.user_id IN (?) AND lc.is_deleted = 0
    GROUP BY l.user_id
`
	query, args, err = sqlx.In(queryTips, userIDs)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to build tips query: "+err.Error())
	}
	query = tx.Rebind(query)
	rows, err = tx.QueryxContext(ctx, query, args...)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to fetch tips: "+err.Error())
	}
	defer rows.Close()

	for rows.Next() {
		var userID, totalTips int64
		if err := rows.Scan(&userID, &totalTips); err != nil {
			return echo.NewHTTPError(http.StatusInternalServerError, "failed to scan tips: "+err.Error())
		}
		tipsMap[userID] = totalTips
	}

	// reactionsとtipsを用いてスコア計算
	for _, user := range users {
		reactions := reactionsMap[user.ID]
		totalTips := tipsMap[user.ID]
		score := reactions + totalTips

		ranking = append(ranking, UserRankingEntry{
			Username: user.Name,
			Score:    score,
		})
	}

	sort.Sort(ranking)

	var rank int64 = 1
	for i := len(ranking) - 1; i >= 0; i-- {
		entry := ranking[i]
		if entry.Username == username {
			break
		}
		rank++
	}

	// リアクション数
	var totalReactions int64
	query = `SELECT COUNT(*) FROM users u 
    INNER JOIN livestreams l ON l.user_id = u.id 
    INNER JOIN reactions r ON r.livestream_id = l.id
    WHERE u.name = ?
	`
	if err := tx.GetContext(ctx, &totalReactions, query, username); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to count total reactions: "+err.Error())
	}

	// ライブコメント数、チップ合計
	var totalLivecomments int64
	var totalTip int64
	var livestreams []*LivestreamModel
	if err := tx.SelectContext(ctx, &livestreams, "SELECT * FROM livestreams WHERE user_id = ?", user.ID); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to get livestreams: "+err.Error())
	}

	for _, livestream := range livestreams {
		var livecomments []*LivecommentModel
		if err := tx.SelectContext(ctx, &livecomments, "SELECT * FROM livecomments WHERE livestream_id = ? and is_deleted = 0", livestream.ID); err != nil && !errors.Is(err, sql.ErrNoRows) {
			return echo.NewHTTPError(http.StatusInternalServerError, "failed to get livecomments: "+err.Error())
		}

		for _, livecomment := range livecomments {
			totalTip += livecomment.Tip
			totalLivecomments++
		}
	}

	// 合計視聴者数
	var viewersCount int64
	for _, livestream := range livestreams {
		var cnt int64
		if err := tx.GetContext(ctx, &cnt, "SELECT COUNT(*) FROM livestream_viewers_history WHERE livestream_id = ?", livestream.ID); err != nil && !errors.Is(err, sql.ErrNoRows) {
			return echo.NewHTTPError(http.StatusInternalServerError, "failed to get livestream_view_history: "+err.Error())
		}
		viewersCount += cnt
	}

	// お気に入り絵文字
	var favoriteEmoji string
	query = `
	SELECT r.emoji_name
	FROM users u
	INNER JOIN livestreams l ON l.user_id = u.id
	INNER JOIN reactions r ON r.livestream_id = l.id
	WHERE u.name = ?
	GROUP BY emoji_name
	ORDER BY COUNT(*) DESC, emoji_name DESC
	LIMIT 1
	`
	if err := tx.GetContext(ctx, &favoriteEmoji, query, username); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to find favorite emoji: "+err.Error())
	}

	stats := UserStatistics{
		Rank:              rank,
		ViewersCount:      viewersCount,
		TotalReactions:    totalReactions,
		TotalLivecomments: totalLivecomments,
		TotalTip:          totalTip,
		FavoriteEmoji:     favoriteEmoji,
	}
	return c.JSON(http.StatusOK, stats)
}

func getLivestreamStatisticsHandler(c echo.Context) error {
	ctx := c.Request().Context()

	if err := verifyUserSession(c); err != nil {
		return err
	}

	id, err := strconv.Atoi(c.Param("livestream_id"))
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "livestream_id in path must be integer")
	}
	livestreamID := int64(id)

	tx, err := dbConn.Connx(ctx)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to get connection: "+err.Error())
	}
	defer tx.Close()

	var livestream LivestreamModel
	if err := tx.GetContext(ctx, &livestream, "SELECT * FROM livestreams WHERE id = ?", livestreamID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return echo.NewHTTPError(http.StatusBadRequest, "cannot get stats of not found livestream")
		} else {
			return echo.NewHTTPError(http.StatusInternalServerError, "failed to get livestream: "+err.Error())
		}
	}

	var livestreams []*LivestreamModel
	if err := tx.SelectContext(ctx, &livestreams, "SELECT * FROM livestreams"); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to get livestreams: "+err.Error())
	}

	// ランク算出
	var ranking LivestreamRanking

	// livestreamsのIDリストを抽出
	livestreamIDs := make([]int64, 0, len(livestreams))
	for _, livestream := range livestreams {
		livestreamIDs = append(livestreamIDs, livestream.ID)
	}

	// reactionsを一度に取得
	reactionsMap := make(map[int64]int64)
	queryReactions := `
    SELECT livestream_id, COUNT(*) as reactions_count
    FROM reactions
    WHERE livestream_id IN (?)
    GROUP BY livestream_id
`
	query, args, err := sqlx.In(queryReactions, livestreamIDs)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to build reactions query: "+err.Error())
	}
	query = tx.Rebind(query)
	rows, err := tx.QueryxContext(ctx, query, args...)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to fetch reactions: "+err.Error())
	}
	defer rows.Close()

	for rows.Next() {
		var livestreamID, reactions int64
		if err := rows.Scan(&livestreamID, &reactions); err != nil {
			return echo.NewHTTPError(http.StatusInternalServerError, "failed to scan reactions: "+err.Error())
		}
		reactionsMap[livestreamID] = reactions
	}

	// tipsを一度に取得
	tipsMap := make(map[int64]int64)
	queryTips := `
    SELECT livestream_id, IFNULL(SUM(tip), 0) as total_tips
    FROM livecomments
    WHERE livestream_id IN (?) and is_deleted = 0
    GROUP BY livestream_id
`
	query, args, err = sqlx.In(queryTips, livestreamIDs)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to build tips query: "+err.Error())
	}
	query = tx.Rebind(query)
	rows, err = tx.QueryxContext(ctx, query, args...)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to fetch tips: "+err.Error())
	}
	defer rows.Close()

	for rows.Next() {
		var livestreamID, totalTips int64
		if err := rows.Scan(&livestreamID, &totalTips); err != nil {
			return echo.NewHTTPError(http.StatusInternalServerError, "failed to scan tips: "+err.Error())
		}
		tipsMap[livestreamID] = totalTips
	}

	// reactionsとtipsを用いてスコア計算
	for _, livestream := range livestreams {
		reactions := reactionsMap[livestream.ID]
		totalTips := tipsMap[livestream.ID]
		score := reactions + totalTips

		ranking = append(ranking, LivestreamRankingEntry{
			LivestreamID: livestream.ID,
			Score:        score,
		})
	}

	sort.Sort(ranking)

	var rank int64 = 1
	for i := len(ranking) - 1; i >= 0; i-- {
		entry := ranking[i]
		if entry.LivestreamID == livestreamID {
			break
		}
		rank++
	}

	// 視聴者数算出
	var viewersCount int64
	if err := tx.GetContext(ctx, &viewersCount, `SELECT COUNT(*) FROM livestreams l INNER JOIN livestream_viewers_history h ON h.livestream_id = l.id WHERE l.id = ?`, livestreamID); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to count livestream viewers: "+err.Error())
	}

	// 最大チップ額
	var maxTip int64
	if err := tx.GetContext(ctx, &maxTip, `SELECT IFNULL(MAX(tip), 0) FROM livestreams l INNER JOIN livecomments l2 ON l2.livestream_id = l.id WHERE l.id = ? and l2.is_deleted = 0`, livestreamID); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to find maximum tip livecomment: "+err.Error())
	}

	// リアクション数
	var totalReactions int64
	if err := tx.GetContext(ctx, &totalReactions, "SELECT COUNT(*) FROM livestreams l INNER JOIN reactions r ON r.livestream_id = l.id WHERE l.id = ?", livestreamID); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to count total reactions: "+err.Error())
	}

	// スパム報告数
	var totalReports int64
	if err := tx.GetContext(ctx, &totalReports, `SELECT COUNT(*) FROM livestreams l INNER JOIN livecomment_reports r ON r.livestream_id = l.id WHERE l.id = ?`, livestreamID); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to count total spam reports: "+err.Error())
	}

	return c.JSON(http.StatusOK, LivestreamStatistics{
		Rank:           rank,
		ViewersCount:   viewersCount,
		MaxTip:         maxTip,
		TotalReactions: totalReactions,
		TotalReports:   totalReports,
	})
}
