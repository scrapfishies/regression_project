# Code Review

## TA: Chris Doenlen

## Notes

* README page / Repo organization
  * Consider making your title the title instead of naming a section "Project Title" and listing it under there
  * Description is light -- could use more of a write up to capture attention and why we should be interested
  * Results section is light, too -- is this a good model? What are your key findings/takeaways? 
* Scraping
  * I like that each site has it's own Python file of scraping functions -- keeps things clean / more organized
  * I could simply be missing this, but I don't see where you used `thenumbers_scraper.py` though -- just `boxofficemojo_scraper.py`
  * the `generate_files.ipynb` notebook could be part of the scraper files since its just saving data
  * Would like to see more documentation (docstrings, commenting) about what's happening in the file
* Main work notebook: `box_office_prediction.ipynb`
  * Missing EDA? 
  * Data cleaning? 
  * Regression modeling
    * Train/Test split done manually - this is fine, but could also use sklearn
    * No validation set -- only uses train and test sets
    * `score_df` function only gives test r2 score -- we need both train and test scores to decide whether or not there is a good fit happening
    * Could have explored some regularization models and feature engineering to get more out of the model
    * Little to no write up in terms of process or decisions being made. Would like to see more of an introduction, analysis of results, etc.
    
  
