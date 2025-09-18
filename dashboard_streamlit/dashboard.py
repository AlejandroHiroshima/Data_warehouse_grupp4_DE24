import streamlit as st    
from connect_data_warehouse import query_job_listings
import plotly.express as px

def layout():
    st.set_page_config(layout="wide")
    st.title("Yrkesfakta vägledaren")
    # st.write("Filtrera på yrkeskategori")
    choices =  {'Pedagogik':'mart_p', 
                'Säkerhet och bevakning':'mart_sb', 
                'Transport, distribution, lager':'mart_tdl'}
    selected_occupation_field = st.selectbox("Välj yrkeskategori", options = choices.keys(), index= None)
    if selected_occupation_field:
        df = query_job_listings(query=f"select * from {choices[selected_occupation_field]}")
        
        cols = st.columns(3)
        with cols[0]:
            st.metric(label="Total antal annonser", value=int(df["VACANCIES"].sum()))
            st.write(f"Top 10 flest yrkesroller för {selected_occupation_field}")
            table = choices[selected_occupation_field]
            df_roles = query_job_listings(f"""
                SELECT
                    OCCUPATION AS "Yrkestitel",
                    SUM(VACANCIES)        AS "Annonser"
                FROM {table}
                GROUP BY OCCUPATION
                ORDER BY "Annonser" DESC
                LIMIT 10
            """)
            st.dataframe(df_roles, hide_index=True)

  
        
        with cols[1]:
                df = query_job_listings(f"""
            SELECT
                SUM(VACANCIES) AS "Annonser",
                employer_name  AS "Arbetsgivare"
            FROM {choices[selected_occupation_field]}
            GROUP BY 2
            ORDER BY 1 DESC
            LIMIT 10
        """)
                fig = px.bar(df, x="Arbetsgivare", y="Annonser")

                # Gör axelrubriker feta och centrera titel
                fig.update_layout(
                xaxis_title="<b>Arbetsgivare</b>",
                yaxis_title="<b>Annonser</b>",
                title=dict(
                text="Top 10 företag med flest annonser",
                x=0.5,  # centrera titel
                xanchor="center"
    )
)
                st.plotly_chart(fig, use_container_width=True)
            
    
        with cols[2]:
            table = choices[selected_occupation_field]
            df = query_job_listings(f"""
            SELECT
                workplace_region AS "Region",
                SUM(vacancies)   AS "Annonser"
            FROM {table}
            GROUP BY workplace_region
            ORDER BY "Annonser" DESC
            LIMIT 5
        """)
            fig = px.bar(df, x="Region", y="Annonser")

            # Gör axelrubriker feta och centrera titel
            fig.update_layout(
            xaxis_title="<b>Region</b>",
            yaxis_title="<b>Antal annonser</b>",
            title=dict(
            text="Top 5 regioner med flest annonser",
            x=0.5,  # centrera titel
            xanchor="center"
    )
)
            st.plotly_chart(fig, use_container_width=True)


        
        st.markdown("## Find advertisement")   
        
        cols = st.columns(2)
        
        with cols[0]:
            selected_company = st.selectbox("Select a company:", df["EMPLOYER_NAME"].unique())    
            
        with cols[1]:
            selected_headline = st.selectbox("Select an advertisement:", df.query ("EMPLOYER_NAME == @selected_company")["HEADLINE"],)  
        
        st.markdown("Job ad")   
        st.markdown(
            df.query("HEADLINE == @selected_headline and EMPLOYER_NAME == @selected_company")["DESCRIPTION_HTML_FORMATTED"].values[0], unsafe_allow_html=True)
    
    
        st.markdown("## Job listings data")   
        
    
    # st.dataframe(df)

        
    
    
if __name__ == "__main__":
    layout()