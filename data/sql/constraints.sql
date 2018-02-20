ALTER TABLE Claim  ADD  CONSTRAINT FK_DrugProduct_DrugProductId FOREIGN KEY(ProductCodeId)
REFERENCES DrugProduct (DrugProductId)

ALTER TABLE Claim  ADD  CONSTRAINT FK_Indication_IndicationId FOREIGN KEY(IndicationId)
REFERENCES Indication (IndicationId)

ALTER TABLE DrugProduct  ADD  CONSTRAINT FK_DrugProductFamily_DrugProductFamilyId FOREIGN KEY(DrugProductFamilyId)
REFERENCES DrugProductFamily (DrugProductFamilyId)
