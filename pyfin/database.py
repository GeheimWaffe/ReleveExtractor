from sqlalchemy import create_engine, select
from sqlalchemy.orm import DeclarativeBase, Mapped, Session
from sqlalchemy.orm import mapped_column
from sqlalchemy.types import String


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