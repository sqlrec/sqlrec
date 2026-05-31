create or replace sql function diversify_fun;

define input table rec_item(
  movie_id bigint,
  genres array<string>,
  title string,
  rec_reason string
);

cache table diversify_rec_item as call window_diversify(rec_item, 'genres', '3', '1', '10');

return diversify_rec_item;
