from typing import List

from sqlalchemy import create_engine, select, Numeric, Date, Boolean, ForeignKey, and_, not_
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, relationship
from sqlalchemy.orm import mapped_column
from sqlalchemy.types import String, Integer
from datetime import datetime, date
from sqlalchemy.sql.functions import max


class Base(DeclarativeBase):
    pass


class MapOrganisme(Base):
    __tablename__ = 'map_organismes'

    keyword: Mapped[str] = mapped_column('Keyword', String, primary_key=True,
                                         comment="Le mot-clé à chercher dans la transaction")
    organisme: Mapped[str] = mapped_column('Organisme', String,
                                           comment="Organisme qui fournit une prestation de remboursement médical")

    def __repr__(self):
        return f'Map Organisme : keyword {self.keyword} to {self.organisme}'


class MapCategorie(Base):
    __tablename__ = 'map_categories'

    keyword: Mapped[str] = mapped_column('Keyword', String, primary_key=True,
                                         comment="Le mot-clé à chercher dans la transaction")
    categorie: Mapped[str] = mapped_column('Catégorie', String,
                                           comment="La catégorie sur laquelle on va mapper la transaction")

    def __repr__(self):
        return f'Map {self.keyword} to {self.categorie}'


class Job(Base):
    __tablename__ = 'jobs'

    job_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    job_key: Mapped[str]
    job_timestamp: Mapped[datetime]

    mouvements: Mapped[List["Mouvement"]] = relationship(back_populates='job', cascade='all, delete-orphan')


class Mouvement(Base):
    __tablename__ = 'comptes'

    index: Mapped[int] = mapped_column('index', Integer, primary_key=True)
    date: Mapped[datetime] = mapped_column('Date', Date, nullable=True)
    description: Mapped[str] = mapped_column('Description', String)
    recette: Mapped[float] = mapped_column('Recette', Numeric, nullable=True)
    depense: Mapped[float] = mapped_column('Dépense', Numeric, nullable=True)
    compte: Mapped[str] = mapped_column('Compte', String, nullable=True)
    categorie: Mapped[str] = mapped_column('Catégorie', String)
    economie: Mapped[str] = mapped_column('Economie', String, nullable=True, default='false')
    regle: Mapped[str] = mapped_column('Réglé', String, nullable=True, default='false')
    mois: Mapped[datetime] = mapped_column('Mois', Date, comment='Le mois auquel se réfère la transaction')
    date_insertion: Mapped[datetime] = mapped_column("Date insertion", Date)
    provision_payer: Mapped[float] = mapped_column('Provision à payer', Numeric, nullable=True)
    provision_recuperer: Mapped[float] = mapped_column('Provision à récupérer', Numeric, nullable=True)
    date_remboursement: Mapped[datetime] = mapped_column('Date remboursement', Date, nullable=True)
    organisme: Mapped[str] = mapped_column('Organisme', String, nullable=True)
    date_out_of_bound: Mapped[bool] = mapped_column('Date Out of Bound', Boolean, nullable=True, default=False)
    taux_remboursement: Mapped[float] = mapped_column('Taux remboursement', Numeric, nullable=True)
    fait_marquant: Mapped[str] = mapped_column('Fait marquant', String, nullable=True)
    no: Mapped[int] = mapped_column('No', Integer)
    no_de_reference: Mapped[str] = mapped_column('Numéro de référence', String, nullable=True)
    index_parent: Mapped[int] = mapped_column('index parent', Integer, nullable=True, comment='Parent transaction')
    depense_initiale: Mapped[float] = mapped_column('Dépense initiale', Numeric, nullable=True,
                                                    comment="La dépense d'origine")
    recette_initiale: Mapped[float] = mapped_column('Recette initiale', Numeric, nullable=True,
                                                    comment="La recette d'origine")
    label_utilisateur: Mapped[str] = mapped_column('Label utilisateur', String, nullable=True)
    job_id: Mapped[int] = mapped_column(ForeignKey('jobs.job_id'))

    job: Mapped[Job] = relationship(back_populates="mouvements")

    def __repr__(self):
        if self.compte is None:
            typ = 'Provision'
        elif self.categorie is None:
            typ = 'Virement'
        else:
            typ = 'Transaction'
        solde = self.recette if self.recette != None else 0
        solde = solde - self.depense if self.depense != None else solde

        return (
            f'{typ} {self.description!r} on {self.date}, Compte : {self.compte}, Catégorie : {self.categorie}, Solde : {solde}'
            f', Provisions : {self.provision_recuperer}'
            f', Number : {self.no}'
            f', N° de référence : {self.no_de_reference}'
            f', Organisme : {self.organisme}')

    def is_cheque(self) -> bool:
        return self.no_de_reference is not None

    def get_depense(self) -> float:
        return 0 if self.depense is None else self.depense

    def get_recette(self) -> float:
        return 0 if self.recette is None else self.recette

    def get_solde(self) -> float:
        return self.get_recette() - self.get_depense()

    def get_depense_initiale(self) -> float:
        return 0 if self.depense_initiale is None else self.depense_initiale

    def get_recette_initiale(self) -> float:
        return 0 if self.recette_initiale is None else self.recette_initiale

    def get_solde_initial(self) -> float:
        return self.get_recette_initiale() - self.get_depense_initiale()


def equal_floats(a, b) -> bool:
    return round(float(a), 2) == round(float(b), 2)


def is_equal_amount_category_compte(a: Mouvement, b: Mouvement) -> bool:
    return ((equal_floats(a.get_solde(), b.get_solde()) or equal_floats(a.get_solde_initial(),
                                                                        b.get_solde())) and a.categorie == b.categorie and a.compte == b.compte)


def is_equal_amount_compte(a: Mouvement, b: Mouvement) -> bool:
    return (equal_floats(a.get_solde(), b.get_solde()) or equal_floats(a.get_solde_initial(),
                                                                       b.get_solde())) and a.compte == b.compte


def get_finance_engine():
    return create_engine("postgresql://regular_user:userpassword@localhost:5432/finance")


def get_map_organismes():
    e = get_finance_engine()
    with Session(e) as session:
        result = session.scalars(select(MapOrganisme)).all()

    return result


def get_map_categories():
    e = get_finance_engine()
    with Session(e) as session:
        result = session.scalars(select(MapCategorie)).all()

    return result


def get_last_updates_by_account():
    """ Returns an array with tuples of (date, compte)
    so that I can filter the last expenses properly"""
    e = get_finance_engine()

    with Session(e) as session:
        result = session.execute(select(max(Mouvement.date_insertion), Mouvement.compte).group_by(Mouvement.compte)).all()

    if result is None:
        return []
    else:
        return result

def create_new_job_import() -> Job:
    """ Initialises a new job with a specific keyword"""
    return Job(job_key='import', job_timestamp=datetime.now())


def get_mouvements(start_date: date, end_date: date, transaction_only: bool = False) -> list:
    # Build the statement
    stmt = select(Mouvement).where(and_(Mouvement.date >= start_date, Mouvement.date <= end_date)).where()

    if transaction_only:
        stmt = stmt.where(not_(and_(Mouvement.depense == None, Mouvement.recette == None)))

    # execute the query
    e = get_finance_engine()
    with Session(e) as session:
        result = session.scalars(stmt).all()

    return [m for m in result]
