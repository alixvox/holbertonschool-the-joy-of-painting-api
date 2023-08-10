-- Drop tables if they exist
DROP TABLE IF EXISTS Episode_Subject;
DROP TABLE IF EXISTS Episode_Color;
DROP TABLE IF EXISTS Episode;
DROP TABLE IF EXISTS SubjectMatter;
DROP TABLE IF EXISTS ColorPalette;

-- Create the ColorPalette table
CREATE TABLE ColorPalette (
    ColorID INTEGER PRIMARY KEY,
    ColorName TEXT NOT NULL,
    HexValue TEXT NOT NULL
);

-- Create the SubjectMatter table
CREATE TABLE SubjectMatter (
    SubjectID INTEGER PRIMARY KEY,
    SubjectName TEXT NOT NULL
);

-- Create the Episode table
CREATE TABLE Episode (
    EpisodeID INTEGER PRIMARY KEY,
    Title TEXT NOT NULL,
    BroadcastDate TEXT NOT NULL,  -- SQLite does not have a DATE type, so we use TEXT
    ImageSrc TEXT,
    YouTubeSrc TEXT,
    Season INTEGER NOT NULL,
    EpisodeInSeason INTEGER NOT NULL,
    TotalEpisodeNum INTEGER NOT NULL  -- This represents the episode number in total of all episodes
);

-- Create the Episode-Color relationship table
CREATE TABLE Episode_Color (
    EpisodeID INTEGER REFERENCES Episode(EpisodeID),
    ColorID INTEGER REFERENCES ColorPalette(ColorID),
    PRIMARY KEY (EpisodeID, ColorID)
);

-- Create the Episode-Subject relationship table
CREATE TABLE Episode_Subject (
    EpisodeID INTEGER REFERENCES Episode(EpisodeID),
    SubjectID INTEGER REFERENCES SubjectMatter(SubjectID),
    PRIMARY KEY (EpisodeID, SubjectID)
);
