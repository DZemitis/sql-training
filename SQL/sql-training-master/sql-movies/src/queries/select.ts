import { ACTORS, DIRECTORS, GENRES, KEYWORDS, MOVIES, MOVIE_RATINGS, PRODUCTION_COMPANIES } from "../table-names";

export const selectActorByName = (fullName: string): string => {
  return (
    `SELECT *
    FROM ${ACTORS}
    WHERE full_name = "${fullName}"`
  );
};

export const selectKeyword = (keyword: string): string => {
  return (
    `SELECT *
    FROM ${KEYWORDS}
    WHERE keyword = "${keyword}"`
  );
};

export const selectDirector = (director: string): string => {
  return (
    `SELECT *
    FROM ${DIRECTORS}
    WHERE full_name = "${director}"`
  );
};

export const selectGenre = (genre: string): string => {
  return (
    `SELECT *
    FROM ${GENRES}
    WHERE genre = "${genre}"`
  );
};

export const selectProductionCompany = (company: string): string => {
  return (
    `SELECT *
    FROM ${PRODUCTION_COMPANIES}
    WHERE company_name = "${company}"`
  );
};

export const selectMovie = (imdbId: string): string => {
  return (
    `SELECT *
    FROM ${MOVIES}
    WHERE imdb_id = "${imdbId}"`
  );
};

export const selectMovieById = (id: number): string => {
  return (
    `SELECT *
    FROM ${MOVIES}
    WHERE id = ${id}`
  );
};

export const selectGenreById = (id: number): string => {
  return (
    `SELECT *
    FROM ${GENRES}
    WHERE id = ${id}`
  );
};

export const selectDirectorById = (id: number): string => {
  return (
    `SELECT *
    FROM ${DIRECTORS}
    WHERE id = ${id}`
  );
};

export const selectActorById = (id: number): string => {
  return (
    `SELECT *
    FROM ${ACTORS}
    WHERE id = ${id}`
  );
};

export const selectKeywordById = (id: number): string => {
  return (
    `SELECT *
    FROM ${KEYWORDS}
    WHERE id = ${id}`
  );
};

export const selectProductionCompanyById = (id: number): string => {
  return (
    `SELECT *
    FROM ${PRODUCTION_COMPANIES}
    WHERE id = ${id}`
  );
};

export const selectMovieId = (imdbId: string): string => {
  return (
    `SELECT id
    FROM ${MOVIES}
    WHERE imdb_id = '${imdbId}'`
  );
};

export const selectRatingsByUserID = (userId: number): string => {
  return (
    `SELECT *
    FROM ${MOVIE_RATINGS}
    WHERE user_id = ${userId}`
  );
};

export const selectGenresByMovieId = (movieId: number): string => {
  return `select g.genre from movie_genres mg join genres g on g.id = mg.genre_id where mg.movie_id = ${movieId}`;
};

export const selectActorsByMovieId = (movieId: number): string => {
  return `select a.full_name from movie_actors ma join actors a on a.id = ma.actor_id where ma.movie_id = ${movieId}`;
};

export const selectDirectorsByMovieId = (movieId: number): string => {
  return `select d.full_name from movie_directors md join directors d on d.id = md.director_id where md.movie_id = ${movieId}`;
};

export const selectKeywordsByMovieId = (movieId: number): string => {
  return `select k.keyword from movie_keywords mk join keywords k on k.id = mk.keyword_id where mk.movie_id = ${movieId}`;
};

export const selectProductionCompaniesByMovieId = (movieId: number): string => {
  return `select pc.company_name from movie_production_companies mpc join production_companies pc on pc.id = mpc.company_id where mpc.movie_id = ${movieId}`;
};

/**
 * select count as c, because an object is returned and expected property name is c
 */
export const selectCount = (table: string): string => {
  return (
    `SELECT COUNT(*) AS c
    FROM ${table}`
  );
};
