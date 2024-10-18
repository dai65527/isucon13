package main

import (
	"net/http"

	"github.com/labstack/echo/v4"
)

type Tag struct {
	ID   int64  `json:"id"`
	Name string `json:"name"`
}

type TagModel struct {
	ID   int64  `db:"id"`
	Name string `db:"name"`
}

type TagsResponse struct {
	Tags []*Tag `json:"tags"`
}

func getTagHandler(c echo.Context) error {
	ctx := c.Request().Context()

	tx, err := dbConn.Connx(ctx)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to get connection: "+err.Error())
	}
	defer tx.Close()

	var tagModels []*TagModel
	if err := tx.SelectContext(ctx, &tagModels, "SELECT * FROM tags"); err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to get tags: "+err.Error())
	}

	tags := make([]*Tag, len(tagModels))
	for i := range tagModels {
		tags[i] = &Tag{
			ID:   tagModels[i].ID,
			Name: tagModels[i].Name,
		}
	}
	return c.JSON(http.StatusOK, &TagsResponse{
		Tags: tags,
	})
}

// 配信者のテーマ取得API
// GET /api/user/:username/theme
func getStreamerThemeHandler(c echo.Context) error {
	userID, err := verifyUserSessionWithUserID(c)
	if err != nil {
		// echo.NewHTTPErrorが返っているのでそのまま出力
		c.Logger().Printf("verifyUserSession: %+v\n", err)
		return err
	}

	darkmode := themeCache.Get(userID)

	theme := Theme{
		ID:       userID,
		DarkMode: darkmode,
	}

	return c.JSON(http.StatusOK, theme)
}
