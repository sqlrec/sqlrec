/*
 * diversify_fun: post-ranking diversification.
 * Uses window_diversify to ensure no single genre dominates the top-K results.
 */
create or replace sql function diversify_fun;

define input table rec_item(
  movie_id bigint,
  genres array<string>,
  title string,
  rec_reason string
);

-- window_diversify: (table, group_col, window_size, min_per_group, max_per_group)
cache table diversify_rec_item as call window_diversify(rec_item, 'genres', '3', '1', '10');

return diversify_rec_item;
