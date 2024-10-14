USE `isupipe`;

alter table icons add index idx_icons_userid (user_id);
alter table livestream_tags add index idx_livestreamtags_livestreamid (livestream_id);
alter table themes add index idx_themes_userid (user_id);
alter table livecomments add index idx_livecomments_livestreamid (livestream_id);