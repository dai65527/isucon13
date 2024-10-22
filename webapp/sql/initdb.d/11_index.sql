USE `isupipe`;

alter table icons add index idx_icons_userid (user_id);
alter table livestream_tags add index idx_livestreamtags_livestreamid (livestream_id);
alter table themes add index idx_themes_userid (user_id);
alter table reactions add index idx_reactions_livestreamid (livestream_id);
alter table ng_words add index idx_ngwords_livestreamid (livestream_id);
alter table livecomments add column `is_deleted` tinyint(1) default 0;
alter table livecomments add index idx_livecomments_livestreamid_isdeleted_createdat (livestream_id, is_deleted, created_at desc);
alter table reservation_slots add index idx_reservationslots_startat (start_at);
