import KcAdminClient from '@keycloak/keycloak-admin-client';

import config from './config.js';
import database from './database.js';

import express from 'express';
import bodyParser from 'body-parser';
import { gql } from 'graphql-request';
import Minio from 'minio';
import createReport from 'docx-templates';
import fetch from 'node-fetch';
import toPdf from 'office-to-pdf';
import superagent from 'superagent';

const app = express();
const port = 3000;


const kcAdminClient = new KcAdminClient.default({
  baseUrl: config.keycloak.url,
  realmName: config.keycloak.realm,
});

const minioClient = new Minio.Client({
  endPoint: config.minio.url,
  useSSL: false,
  port: 9000,
  accessKey: config.minio.accessKey,
  secretKey: config.minio.secretKey
});

app.use(bodyParser.json({ limit: '500mb' }));
app.use(bodyParser.urlencoded({ limit: '500mb', extended: true }));


app.post("/segnalazione/insert", async (req, res) => {
  const input = req.body.input;
  if (input.segnalazione.allegati) {
    input.segnalazione.allegati.data.map(allegato => {
      if (allegato.delete) {
        minioClient.removeObject('segnalazione-' + input.segnalazione.id, allegato.nome, function (err, etag) {
          if (err) console.log('Unable to remove object', err)
          console.log('Removed the object')
        });
      } else {
        var file = new Buffer.from(allegato.file.split(',')[1], 'base64');
        minioClient.putObject('segnalazione-' + input.segnalazione.id, 'segnalazione/' + allegato.nome, file, {
          'Content-Type': allegato.tipo,
        }, function (err, objInfo) {
          if (err) return console.log(err) // err should be null
          console.log("Success", objInfo)
        })
      }
    })
    delete input.segnalazione.allegati;
  }

  if (input.segnalazione.intervento && input.segnalazione.intervento.allegati) {
    input.segnalazione.intervento.allegati.data.map(allegato => {
      if (allegato.delete) {
        minioClient.removeObject('segnalazione-' + input.segnalazione.id, allegato.nome, function (err, etag) {
          if (err) console.log('Unable to remove object', err)
          console.log('Removed the object')
        });
      } else {
        var file = new Buffer.from(allegato.file.split(',')[1], 'base64');
        minioClient.putObject('segnalazione-' + input.segnalazione.id, 'intervento/' + allegato.nome, file, {
          'Content-Type': allegato.tipo,
        }, function (err, objInfo) {
          if (err) return console.log(err) // err should be null
          console.log("Success", objInfo)
        })
      }
    })
    delete input.segnalazione.intervento.allegati;
  }
  const mutation = gql`
  mutation UpdateSegnalazione(
    $segnalazione: [pis_segnalazione_insert_input!] = {}
    $on_conflict: pis_segnalazione_on_conflict = { constraint: segnalazione_pkey }
  ) {
    insert_pis_segnalazione(objects: $segnalazione, on_conflict: $on_conflict) {
      returning {
        id
        
      }
    }
  }
`;
  let response = await database.queryFetch(mutation, {
    segnalazione: input.segnalazione,
    on_conflict: input.on_conflict
  });
  res.send({
    segnalazione_id: response.insert_pis_segnalazione.returning[0].id,
  });
});




app.post("/intervento/straordinario/insert", async (req, res) => {
  const input = req.body.input;

  if (input.intervento_straordinario.allegati) {
    input.intervento_straordinario.allegati.data.map(allegato => {
      if (allegato.delete) {
        minioClient.removeObject('intervento-straordinaio-' + input.intervento_straordinario.id, allegato.nome, function (err, etag) {
          if (err) console.log('Unable to remove object', err)
          console.log('Removed the object')
        });
      } else {
        var file = new Buffer.from(allegato.file.split(',')[1], 'base64');
        minioClient.putObject('intervento-straordinaio-' + input.intervento_straordinario.id, 'allegati/' + allegato.nome, file, {
          'Content-Type': allegato.tipo,
        }, function (err, objInfo) {
          if (err) return console.log(err) // err should be null
          console.log("Success", objInfo)
        })
      }
    })
    delete input.intervento_straordinario.allegati;
  }

  const mutation = gql`
  mutation UpdateInterventoStraordinario(
    $intervento_straordinario: [pis_intervento_straordinario_insert_input!] = {}
    $on_conflict: pis_intervento_straordinario_on_conflict = { constraint: intervento_straordinario_pkey }
  ) {
    insert_pis_intervento_straordinario(objects: $intervento_straordinario, on_conflict: $on_conflict) {
      returning {
        id
      }
    }
  }
`;
  let response = await database.queryFetch(mutation, {
    intervento_straordinario: input.intervento_straordinario,
    on_conflict: input.on_conflict
  });
  res.send({
    intervento_straordinario_id: response.insert_pis_intervento_straordinario.returning[0].id,
  });
});



app.post("/diario/allegato/insert", async (req, res) => {
  const input = req.body.input;
  if (input.objects.allegato) {
    const allegato = input.objects.allegato.data;
    var file = new Buffer.from(allegato.file.split(',')[1], 'base64');
    minioClient.putObject('segnalazione-' + input.objects.segnalazione_id, 'diario/' + allegato.nome, file, {
      'Content-Type': allegato.tipo,
    }, function (err, objInfo) {
      if (err) return console.log(err) // err should be null
      console.log("Success", objInfo)
    })
    input.objects = { ...input.objects, ...{ allegato: allegato.nome } }
  }

  const mutation = gql`
  mutation InsertDiario($objects: [pis_diario_insert_input!] = {}) {
    insert_pis_diario(objects: $objects) {
      affected_rows
    }
  }
`;
  let response = await database.queryFetch(mutation, {
    objects: input.objects
  });
  res.send({
    affected_rows: response.insert_pis_diario.affected_rows,
  });
});


app.post("/segnalazione/protocolla", async (req, res) => {
  const data = req.body.event.data;
  if (data.old.stato != data.new.stato && data.new.stato == 'APERTA') {

    const query = gql
      `query Segnalazione($where: pis_segnalazione_bool_exp!) {
        pis_segnalazione(where: $where) {
        id
        municipalita {
          id
          nome
        }
        quartiere {
          id
          nome
          municipalita {
            municipalita_id
          }
        }
        toponimo {
          id
          nome
          dug {
            id
            nome
          }
          codice
          assegnazioni {
            quartiere_id
          }
        }
        municipalita_storica
        quartiere_storico
        toponimo_storico
        data
        tecnico_referente {
          id
          nome
          cognome
          titolo {
            id
            nome
          }
        }
        priorita {
          id
          nome
        }
        protocollo {
          note
          numero
          mittente {
            id
            codice
            settore
            servizio
            uoc
            uos
            postazione
            nome
          }
          id
          destinatari {
            id
            e_esterno
            destinatario_interno {
              id
              codice
              settore
              servizio
              uoc
              uos
              postazione
              nome
            }
            destinatario_esterno {
              id
              cognome
              email
              codice_fiscale
              nome
            }
          }
          data
        }
        stato
        richiesta_protezione_civile
        dissesto {
          id
          note
          peso
          prima_dimensione
          profondita
          seconda_dimensione
          terza_dimensione
          tipologia {
            id
            nome
            intervento
          }
          forma {
            id
            nome
          }
        }
        posizionamento_toponimo_punto_iniziale {
          civico
          connessione
          geoloc
          id
          ipi
          km
          note
          specifica {
            id
            nome
          }
          tipologia {
            id
            nome
          }
        }
        diario {
          id
          messaggio
          allegato
          utente
        }
        segnalazioni_collegate {
          segnalazione {
            id
            created_at
            protocollo {
              data
              numero
            }
            stato
          }
        }
        eventi {
          stato
          note
          squadra {
            nome
          }
          protocollo {
            numero
            data
            note
          }
          created_at
          risolutore {
            protocollo {
              numero
              data
            }
          }
        }
        intervento {
          id
          assistenza_pm
          condizioni_traffico {
            nome
            id
          }
          attrezzature_impiegate {
            id
            nome
            quantita
          }
          data_fine_lavori
          data_inizio_lavori
          difformita
          dissesto_difforme
          giorni_trascorsi {
            id
            nome
          }
          materiali_dissesto {
            id
            materiale {
              id
              nome
            }
            quantita
            altro
          }
          segnaletica_lasciata {
            id
            nome
            quantita
          }
          note
          veicoli_impiegati {
            id
            targa
          }
        }
      }
    }`;
    let response = await database.queryFetch(query, {
      where: { id: { _eq: data.new.id } },
    });
    const template = await fetch(
      config.template.url + '/segnalazione.docx'
    ).then((v) => v.arrayBuffer());
    const segnalazione = response.pis_segnalazione[0];

    const report = await createReport.default({
      template: template,
      data: segnalazione,
      noSandbox: true,
    });
    var pdfBuffer = await toPdf(report)
    var file = new Buffer.from(pdfBuffer);
    var filename = segnalazione.protocollo.numero.replace(/\//g, '_') + ".pdf";
    minioClient.putObject('segnalazione-' + data.new.id, filename, file, {
      'Content-Type': 'application/pdf',
    }, function (err, objInfo) {
      if (err) return console.log(err) // err should be null
      console.log("Success", objInfo)

      superagent.post(config.protocollo.url + '/aggiungi_allegato').field('protocollo', filename).attach('file', file, { filename: filename }).end(function (error, response) {
        if (error) return console.log(error) // err should be null
        console.log(response);
      })


    });
  }
});

app.get('/_health', (req, res) => {
  res.send({'status': 'ok'}); // Simple health endpoint so kubernetes/other know that service is up and running
});


app.listen(port, () => {
  console.log(`Example app listening at http://localhost:${port}`);
});
