package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/labstack/echo-contrib/session"
	"github.com/labstack/echo/v4"
)

type PostLivecommentRequest struct {
	Comment string `json:"comment"`
	Tip     int64  `json:"tip"`
}

type LivecommentModel struct {
	ID           int64  `db:"id"`
	UserID       int64  `db:"user_id"`
	LivestreamID int64  `db:"livestream_id"`
	Comment      string `db:"comment"`
	Tip          int64  `db:"tip"`
	CreatedAt    int64  `db:"created_at"`
	IsDeleted    bool   `db:"is_deleted"`
}

type Livecomment struct {
	ID         int64      `json:"id"`
	User       User       `json:"user"`
	Livestream Livestream `json:"livestream"`
	Comment    string     `json:"comment"`
	Tip        int64      `json:"tip"`
	CreatedAt  int64      `json:"created_at"`
}

type LivecommentReport struct {
	ID          int64       `json:"id"`
	Reporter    User        `json:"reporter"`
	Livecomment Livecomment `json:"livecomment"`
	CreatedAt   int64       `json:"created_at"`
}

type LivecommentReportModel struct {
	ID            int64 `db:"id"`
	UserID        int64 `db:"user_id"`
	LivestreamID  int64 `db:"livestream_id"`
	LivecommentID int64 `db:"livecomment_id"`
	CreatedAt     int64 `db:"created_at"`
}

type ModerateRequest struct {
	NGWord string `json:"ng_word"`
}

type NGWord struct {
	ID           int64  `json:"id" db:"id"`
	UserID       int64  `json:"user_id" db:"user_id"`
	LivestreamID int64  `json:"livestream_id" db:"livestream_id"`
	Word         string `json:"word" db:"word"`
	CreatedAt    int64  `json:"created_at" db:"created_at"`
}

func getLivecommentsHandler(c echo.Context) error {
	ctx := c.Request().Context()

	if err := verifyUserSession(c); err != nil {
		// echo.NewHTTPErrorが返っているのでそのまま出力
		return err
	}

	livestreamID, err := strconv.Atoi(c.Param("livestream_id"))
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "livestream_id in path must be integer")
	}

	tx, err := dbConn.Connx(ctx)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to get connection: "+err.Error())
	}
	defer tx.Close()

	query := "SELECT * FROM livecomments WHERE livestream_id = ? AND is_deleted = 0 ORDER BY created_at DESC"
	if c.QueryParam("limit") != "" {
		limit, err := strconv.Atoi(c.QueryParam("limit"))
		if err != nil {
			return echo.NewHTTPError(http.StatusBadRequest, "limit query parameter must be integer")
		}
		query += fmt.Sprintf(" LIMIT %d", limit)
	}

	livecommentModels := []LivecommentModel{}
	err = tx.SelectContext(ctx, &livecommentModels, query, livestreamID)
	if errors.Is(err, sql.ErrNoRows) {
		return c.JSON(http.StatusOK, []*Livecomment{})
	}
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to get livecomments: "+err.Error())
	}

	livecomments, err := fillLivecommentResponses(ctx, tx, livecommentModels)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to fill livecomments: "+err.Error())
	}

	return c.JSON(http.StatusOK, livecomments)
}

func getNgwords(c echo.Context) error {
	ctx := c.Request().Context()

	if err := verifyUserSession(c); err != nil {
		return err
	}

	// error already checked
	sess, _ := session.Get(defaultSessionIDKey, c)
	// existence already checked
	userID := sess.Values[defaultUserIDKey].(int64)

	livestreamID, err := strconv.Atoi(c.Param("livestream_id"))
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "livestream_id in path must be integer")
	}

	tx, err := dbConn.Connx(ctx)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to get connection: "+err.Error())
	}
	defer tx.Close()

	var ngWords []*NGWord
	if err := tx.SelectContext(ctx, &ngWords, "SELECT * FROM ng_words WHERE user_id = ? AND livestream_id = ? ORDER BY created_at DESC", userID, livestreamID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return c.JSON(http.StatusOK, []*NGWord{})
		} else {
			return echo.NewHTTPError(http.StatusInternalServerError, "failed to get NG words: "+err.Error())
		}
	}

	return c.JSON(http.StatusOK, ngWords)
}

func postLivecommentHandler(c echo.Context) error {
	ctx := c.Request().Context()
	defer c.Request().Body.Close()

	if err := verifyUserSession(c); err != nil {
		return err
	}

	livestreamID, err := strconv.Atoi(c.Param("livestream_id"))
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "livestream_id in path must be integer")
	}

	// error already checked
	sess, _ := session.Get(defaultSessionIDKey, c)
	// existence already checked
	userID := sess.Values[defaultUserIDKey].(int64)

	var req *PostLivecommentRequest
	if err := json.NewDecoder(c.Request().Body).Decode(&req); err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "failed to decode the request body as json")
	}

	tx, err := dbConn.Connx(ctx)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to get connection: "+err.Error())
	}
	defer tx.Close()

	var livestreamModel LivestreamModel
	if err := tx.GetContext(ctx, &livestreamModel, "SELECT * FROM livestreams WHERE id = ?", livestreamID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return echo.NewHTTPError(http.StatusNotFound, "livestream not found")
		} else {
			return echo.NewHTTPError(http.StatusInternalServerError, "failed to get livestream: "+err.Error())
		}
	}

	// スパム判定
	var ngwords []*NGWord
	if err := tx.SelectContext(ctx, &ngwords, "SELECT id, user_id, livestream_id, word FROM ng_words WHERE livestream_id = ?", livestreamModel.ID); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to get NG words: "+err.Error())
	}

	for _, ngword := range ngwords {
		if strings.Contains(req.Comment, ngword.Word) {
			return echo.NewHTTPError(http.StatusBadRequest, "このコメントがスパム判定されました")
		}
	}

	now := time.Now().Unix()
	livecommentModel := LivecommentModel{
		UserID:       userID,
		LivestreamID: int64(livestreamID),
		Comment:      req.Comment,
		Tip:          req.Tip,
		CreatedAt:    now,
	}

	rs, err := tx.ExecContext(ctx, "INSERT INTO livecomments (user_id, livestream_id, comment, tip, created_at) VALUES (?, ?, ?, ?, ?)", livecommentModel.UserID, livecommentModel.LivestreamID, livecommentModel.Comment, livecommentModel.Tip, livecommentModel.CreatedAt)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to insert livecomment: "+err.Error())
	}

	livecommentID, err := rs.LastInsertId()
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to get last inserted livecomment id: "+err.Error())
	}
	livecommentModel.ID = livecommentID

	livecomment, err := fillLivecommentResponse(ctx, tx, livecommentModel)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to fill livecomment: "+err.Error())
	}

	return c.JSON(http.StatusCreated, livecomment)
}

func reportLivecommentHandler(c echo.Context) error {
	ctx := c.Request().Context()

	if err := verifyUserSession(c); err != nil {
		return err
	}

	livestreamID, err := strconv.Atoi(c.Param("livestream_id"))
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "livestream_id in path must be integer")
	}

	livecommentID, err := strconv.Atoi(c.Param("livecomment_id"))
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "livecomment_id in path must be integer")
	}

	// error already checked
	sess, _ := session.Get(defaultSessionIDKey, c)
	// existence already checked
	userID := sess.Values[defaultUserIDKey].(int64)

	tx, err := dbConn.BeginTxx(ctx, nil)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to begin transaction: "+err.Error())
	}
	defer tx.Rollback()

	var livestreamModel LivestreamModel
	if err := tx.GetContext(ctx, &livestreamModel, "SELECT * FROM livestreams WHERE id = ?", livestreamID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return echo.NewHTTPError(http.StatusNotFound, "livestream not found")
		} else {
			return echo.NewHTTPError(http.StatusInternalServerError, "failed to get livestream: "+err.Error())
		}
	}

	var livecommentModel LivecommentModel
	if err := tx.GetContext(ctx, &livecommentModel, "SELECT * FROM livecomments WHERE id = ?", livecommentID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return echo.NewHTTPError(http.StatusNotFound, "livecomment not found")
		} else {
			return echo.NewHTTPError(http.StatusInternalServerError, "failed to get livecomment: "+err.Error())
		}
	}

	now := time.Now().Unix()
	reportModel := LivecommentReportModel{
		UserID:        int64(userID),
		LivestreamID:  int64(livestreamID),
		LivecommentID: int64(livecommentID),
		CreatedAt:     now,
	}
	rs, err := tx.NamedExecContext(ctx, "INSERT INTO livecomment_reports(user_id, livestream_id, livecomment_id, created_at) VALUES (:user_id, :livestream_id, :livecomment_id, :created_at)", &reportModel)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to insert livecomment report: "+err.Error())
	}
	reportID, err := rs.LastInsertId()
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to get last inserted livecomment report id: "+err.Error())
	}
	reportModel.ID = reportID

	report, err := fillLivecommentReportResponse(ctx, tx, reportModel)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to fill livecomment report: "+err.Error())
	}
	if err := tx.Commit(); err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to commit: "+err.Error())
	}

	return c.JSON(http.StatusCreated, report)
}

// NGワードを登録
func moderateHandler(c echo.Context) error {
	ctx := c.Request().Context()
	defer c.Request().Body.Close()

	if err := verifyUserSession(c); err != nil {
		return err
	}

	livestreamID, err := strconv.Atoi(c.Param("livestream_id"))
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "livestream_id in path must be integer")
	}

	// error already checked
	sess, _ := session.Get(defaultSessionIDKey, c)
	// existence already checked
	userID := sess.Values[defaultUserIDKey].(int64)

	var req *ModerateRequest
	if err := json.NewDecoder(c.Request().Body).Decode(&req); err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "failed to decode the request body as json")
	}

	tx, err := dbConn.BeginTxx(ctx, nil)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to begin transaction: "+err.Error())
	}
	defer tx.Rollback()

	// 配信者自身の配信に対するmoderateなのかを検証
	var ownedLivestreams []LivestreamModel
	if err := tx.SelectContext(ctx, &ownedLivestreams, "SELECT * FROM livestreams WHERE id = ? AND user_id = ?", livestreamID, userID); err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to get livestreams: "+err.Error())
	}
	if len(ownedLivestreams) == 0 {
		return echo.NewHTTPError(http.StatusBadRequest, "A streamer can't moderate livestreams that other streamers own")
	}

	rs, err := tx.NamedExecContext(ctx, "INSERT INTO ng_words(user_id, livestream_id, word, created_at) VALUES (:user_id, :livestream_id, :word, :created_at)", &NGWord{
		UserID:       int64(userID),
		LivestreamID: int64(livestreamID),
		Word:         req.NGWord,
		CreatedAt:    time.Now().Unix(),
	})
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to insert new NG word: "+err.Error())
	}

	wordID, err := rs.LastInsertId()
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to get last inserted NG word id: "+err.Error())
	}

	var ngwords []*NGWord
	if err := tx.SelectContext(ctx, &ngwords, "SELECT * FROM ng_words WHERE livestream_id = ?", livestreamID); err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to get NG words: "+err.Error())
	}

	time.Sleep(500 * time.Millisecond)

	var livecomments []*LivecommentModel
	if err := tx.SelectContext(ctx, &livecomments, "SELECT * FROM livecomments WHERE livestream_id = ? and is_deleted = 0", livestreamID); err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to get livecomments: "+err.Error())
	}

	ng_livecomment_ids := make([]int64, 0, len(livecomments))
	for _, livecomment := range livecomments {
		for _, ngword := range ngwords {
			if strings.Contains(livecomment.Comment, ngword.Word) {
				ng_livecomment_ids = append(ng_livecomment_ids, livecomment.ID)
				break
			}
		}
	}

	// ng_livecomment_idsのコメントを削除
	if len(ng_livecomment_ids) > 0 {
		query, args, err := sqlx.In("UPDATE livecomments SET is_deleted = 1 WHERE id IN (?)", ng_livecomment_ids)
		if err != nil {
			return echo.NewHTTPError(http.StatusInternalServerError, "failed to build delete query: "+err.Error())
		}
		query = tx.Rebind(query)
		_, err = tx.ExecContext(ctx, query, args...)
		if err != nil {
			return echo.NewHTTPError(http.StatusInternalServerError, "failed to delete livecomments: "+err.Error())
		}
	}

	if err := tx.Commit(); err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to commit: "+err.Error())
	}

	return c.JSON(http.StatusCreated, map[string]interface{}{
		"word_id": wordID,
	})
}

func fillLivecommentResponses(ctx context.Context, tx *sqlx.Conn, livecommentModels []LivecommentModel) ([]Livecomment, error) {
	if len(livecommentModels) == 0 {
		return []Livecomment{}, nil
	}

	// Livecomment から UserID と LivestreamID をそれぞれ抽出
	userIDs := make([]int64, len(livecommentModels))
	livestreamIDs := make([]int64, len(livecommentModels))
	for i, livecommentModel := range livecommentModels {
		userIDs[i] = livecommentModel.UserID
		livestreamIDs[i] = livecommentModel.LivestreamID
	}

	// Users を一括取得して fillUserResponses で処理
	var userModels []UserModel
	if len(userIDs) > 0 {
		query, args, err := sqlx.In("SELECT * FROM users WHERE id IN (?)", userIDs)
		if err != nil {
			return nil, err
		}
		query = tx.Rebind(query)
		if err := tx.SelectContext(ctx, &userModels, query, args...); err != nil {
			return nil, fmt.Errorf("failed to get users: %w", err)
		}
	}

	users, err := fillUserResponses(ctx, tx, userModels)
	if err != nil {
		return nil, err
	}

	// ユーザーIDをキーとするUserのマップを作成
	userMap := make(map[int64]User)
	for _, user := range users {
		userMap[user.ID] = user
	}

	// Livestreams を一括取得して fillLivestreamResponses で処理
	var livestreamModels []*LivestreamModel
	if len(livestreamIDs) > 0 {
		query, args, err := sqlx.In("SELECT * FROM livestreams WHERE id IN (?)", livestreamIDs)
		if err != nil {
			return nil, err
		}
		query = tx.Rebind(query)
		if err := tx.SelectContext(ctx, &livestreamModels, query, args...); err != nil {
			return nil, fmt.Errorf("failed to get livestreams: %w", err)
		}
	}

	livestreams, err := fillLivestreamResponses(ctx, tx, livestreamModels)
	if err != nil {
		return nil, fmt.Errorf("failed to fill livestreams: %w", err)
	}

	// ライブストリームIDをキーとするLivestreamのマップを作成
	livestreamMap := make(map[int64]Livestream)
	for _, livestream := range livestreams {
		livestreamMap[livestream.ID] = livestream
	}

	// 最終的な Livecomment のリストを作成
	livecomments := make([]Livecomment, len(livecommentModels))
	for i, livecommentModel := range livecommentModels {
		// ユーザー情報を取得
		commentOwner, ok := userMap[livecommentModel.UserID]
		if !ok {
			return nil, fmt.Errorf("user not found for livecomment id: %d", livecommentModel.ID)
		}

		// ライブストリーム情報を取得
		livestream, ok := livestreamMap[livecommentModel.LivestreamID]
		if !ok {
			return nil, fmt.Errorf("livestream not found for livecomment id: %d", livecommentModel.ID)
		}

		// Livecomment オブジェクトを生成
		livecomments[i] = Livecomment{
			ID:         livecommentModel.ID,
			User:       commentOwner,
			Livestream: livestream,
			Comment:    livecommentModel.Comment,
			Tip:        livecommentModel.Tip,
			CreatedAt:  livecommentModel.CreatedAt,
		}
	}

	return livecomments, nil
}

func fillLivecommentResponse(ctx context.Context, tx SqlxConn, livecommentModel LivecommentModel) (Livecomment, error) {
	commentOwnerModel := UserModel{}
	if err := tx.GetContext(ctx, &commentOwnerModel, "SELECT * FROM users WHERE id = ?", livecommentModel.UserID); err != nil {
		return Livecomment{}, err
	}
	commentOwner, err := fillUserResponse(ctx, tx, commentOwnerModel)
	if err != nil {
		return Livecomment{}, err
	}

	livestreamModel := LivestreamModel{}
	if err := tx.GetContext(ctx, &livestreamModel, "SELECT * FROM livestreams WHERE id = ?", livecommentModel.LivestreamID); err != nil {
		return Livecomment{}, err
	}
	livestream, err := fillLivestreamResponse(ctx, tx, livestreamModel)
	if err != nil {
		return Livecomment{}, err
	}

	livecomment := Livecomment{
		ID:         livecommentModel.ID,
		User:       commentOwner,
		Livestream: livestream,
		Comment:    livecommentModel.Comment,
		Tip:        livecommentModel.Tip,
		CreatedAt:  livecommentModel.CreatedAt,
	}

	return livecomment, nil
}

func fillLivecommentReportResponse(ctx context.Context, tx SqlxConn, reportModel LivecommentReportModel) (LivecommentReport, error) {
	reporterModel := UserModel{}
	if err := tx.GetContext(ctx, &reporterModel, "SELECT * FROM users WHERE id = ?", reportModel.UserID); err != nil {
		return LivecommentReport{}, fmt.Errorf("failed to get reporter: %w", err)
	}
	reporter, err := fillUserResponse(ctx, tx, reporterModel)
	if err != nil {
		return LivecommentReport{}, fmt.Errorf("failed to fill reporter: %w", err)
	}

	livecommentModel := LivecommentModel{}
	if err := tx.GetContext(ctx, &livecommentModel, "SELECT * FROM livecomments WHERE id = ?", reportModel.LivecommentID); err != nil {
		return LivecommentReport{}, fmt.Errorf("failed to get livecomment: %w", err)
	}
	livecomment, err := fillLivecommentResponse(ctx, tx, livecommentModel)
	if err != nil {
		return LivecommentReport{}, fmt.Errorf("failed to fill livecomment: %w", err)
	}

	report := LivecommentReport{
		ID:          reportModel.ID,
		Reporter:    reporter,
		Livecomment: livecomment,
		CreatedAt:   reportModel.CreatedAt,
	}
	return report, nil
}
