package handlers

import (
	"errors"
	"os"

	"github.com/go-gota/gota/dataframe"
)

func Df2Csv(fileLocation string, df dataframe.DataFrame) (ok bool, err error) {
	openingClosingTATOutputFile, err := os.Create(fileLocation)

	if err != nil {
		return false, err
	}

	err = df.WriteCSV(openingClosingTATOutputFile)
	if (df.Error() != nil) || (err != nil) {
		return false, errors.New("could not create csv file")
	}

	return true, nil
}