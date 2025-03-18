# frozen_string_literal: true

require('google/cloud/bigquery')

module Abank
  class Big
    DF = '%Y-%m-%d'

    # @return [Google::Cloud::Bigquery::QueryJob] job bigquery
    # @return [Hash] opcoes trabalho
    # @return [Google::Cloud::Bigquery::Data] resultado do Structured Query Language (SQL) no bigquery
    # @return [Integer] numero linhas afetadas pelo Data Manipulation Language (DML) no bigquery
    # @return [Integer] contrato arrendamento em tratamento
    # @return [Integer] movimento do contrato arrendamento em tratamento
    attr_reader :job, :opcao, :bqres, :bqnrs, :ctpos, :mvpos

    # acesso a base dados abank no bigquery
    # @param [Thor::CoreExt::HashWithIndifferentAccess] opcoes trabalho
    # @option opcoes [String]  :k ('') movimentos a apagar (keysin.mv)
    # @option opcoes [Integer] :n (0) conta apagar movimentos >3 outras (mv)
    # @option opcoes [String]  :c ('') id contrato arrendamento (re)
    # @option opcoes [String]  :d ('') data inicio contrato arrendamento (re)
    # @option opcoes [Boolean] :t (false) trabalha todas as rendas? (re)
    # @return [Hash] opcoes trabalho
    def initialize(opcoes = {})
      @opcao = opcoes
    end

    # insere & classifica movimentos no bigquery
    # @return [Big] acesso a base dados abank no bigquery
    def mv_insert
      return self if mvvls.empty?

      dml("insert #{BD}.mv VALUES#{mvvls.join(',')}")
      puts("MOVIMENTOS INSERIDOS #{bqnrs}")
      mv_classifica if bqnrs.positive?
      self
    end

    # apaga movimentos & suas rendas associadas no bigquery
    # @return [Big] acesso a base dados abank no bigquery
    def mv_delete
      @ctlct = []
      return self if mvkys.empty? && contadel.zero?

      # obtem lista contratos arrendamento associados aos movimentos a apagar
      @ctlct =
        if contadel.zero?
          sql("select distinct ct from #{BD}.gmr where ky IN UNNEST(@kys)", kys: mvkys)
        else
          sql("select distinct ct from #{BD}.gmr where nc=@nc", nc: contadel)
        end
      re_apaga
      mv_delete_dml
      self
    end

    # mostra dados movimentos classificacao
    def mc_show
      if opcao[:c].to_s.empty?
        puts("\ndados movimentos NAO classificados")
        cp1 = sql("select distinct p1 from #{BD}.cc where p1 is not null").map { |p| p[:p1] }
        cp2 = sql("select distinct p2 from #{BD}.cc where p2 is not null").map { |p| p[:p2] }
        sql("select * from #{BD}.gpl where ct is null order by 1 desc limit @lm", lm: opcao[:n])
      else
        puts("\ndados movimentos JA classificados por:")
        cc_show
        cp1 = []
        cp2 = []
        sql("select * from #{BD}.gpl where ct=@ct order by 1 desc limit @lm", lm: opcao[:n], ct: opcao[:c])
      end
      bqres.reject! do |h|
        h[:p1s] = h[:p1s] - cp1
        h[:p2s] = h[:p2s] - cp2 - h[:p1s] - cp1
        h[:p1s].empty? && h[:p2s].empty?
      end
      return if bqres.empty?

      puts('      data         t1    valor palavras p1s/p2s')
      bqres.each { |l| puts(format('%<dl>10s %<t1>10s %<vl>8.2f %<p1>s%<p2>s', dl: l[:dl].strftime(DF), t1: l[:p1], vl: l[:vl], p1: fpls(l[:p1s], 'p1 '), p2: fpls(l[:p2s], '; p2 '))) }
    end

    # mostra classificador
    def cc_show
      if opcao[:c].to_s.empty? && opcao[:p1].empty? && opcao[:p2].empty?
        sql("SELECT * FROM #{BD}.cc")
      elsif opcao[:c].to_s.empty? && opcao[:p1].empty?
        sql("select * from #{BD}.cc WHERE p2 in unnest(@p2s)", p2s: opcao[:p2])
      elsif opcao[:c].to_s.empty? && opcao[:p2].empty?
        sql("select * from #{BD}.cc WHERE p1 in unnest(@p1s)", p1s: opcao[:p1])
      elsif opcao[:p1].empty? && opcao[:p2].empty?
        sql("select * from #{BD}.cc WHERE ct=@ct", ct: opcao[:c])
      elsif opcao[:p1].empty?
        sql("select * from #{BD}.cc WHERE ct=@ct and p2 in unnest(@p2s)", ct: opcao[:c], p2s: opcao[:p2])
      elsif opcao[:p2].empty?
        sql("select * from #{BD}.cc WHERE ct=@ct and p1 in unnest(@p1s)", ct: opcao[:c], p1s: opcao[:p1])
      else
        sql("select * from #{BD}.cc WHERE ct=@ct and p1 in unnest(@p1s) and p2 in unnest(@p2s)", ct: opcao[:c], p1s: opcao[:p1], p2s: opcao[:p2])
      end
      if bqres.empty?
        puts('CLASSIFICADOR NAO EXISTE')
      else
        @bqres = bqres.group_by { |c| c[:ct] }.map { |k, v| {ct: k, p1s: fplo(v)} }
        puts('        id palavras p1s/t1s/p2s ')
        bqres.sort_by { |i| i[:ct] }.each do |l|
          puts(format('%<ct>8s p %<p1>s', ct: l[:ct], p1: fpls(l[:p1s])))
        end
      end
    end

    # apaga classificador
    # @return [Big] acesso a base dados abank no bigquery
    def cc_apaga
      cc_show
      return self if bqres.empty?

      if opcao[:p1].empty? && opcao[:p2].empty?
        dml("delete from #{BD}.cc WHERE ct=@ct", ct: opcao[:c])
      elsif opcao[:p1].empty?
        dml("delete from #{BD}.cc WHERE ct=@ct and p2 IN UNNEST(@p2s)", ct: opcao[:c], p2s: opcao[:p2])
      else
        dml("delete from #{BD}.cc WHERE ct=@ct and p1 IN UNNEST(@p1s)", ct: opcao[:c], p1s: opcao[:p1])
      end
      puts("CLASSIFICADORES APAGADOS #{bqnrs}")
      return self unless opcao[:t]

      dml("update #{BD}.mv set ct=null,p1=null WHERE ct=@ct", ct: opcao[:c])
      puts("MOVIMENTOS DES-CLASSIFICADOS #{bqnrs}")
      self
    end

    # cria classificador
    # @return [Big] acesso a base dados abank no bigquery
    def cc_cria
      vls = opcao[:p1].map.with_index { |p1, i| "('#{opcao[:c]}',#{fpli(p1)},#{fpli(opcao[:p2][i])},#{fpli(opcao[:t1][i])})" }
      dml("insert #{BD}.cc values#{vls.join(',')}")
      puts("CLASSIFICADORES CRIADOS #{bqnrs}")
      self
    end

    # classifica movimentos no bigquery
    # @return [Big] acesso a base dados abank no bigquery
    def mv_classifica
      @ctlct = sql("select * from #{BD}.gnr")
      stp("call #{BD}.uct()")
      puts("MOVIMENTOS CLASSIFICADOS #{bqnrs}")
      self
    end

    # mostra contrato arrendamento
    def ct_mostra
      sql("SELECT * FROM #{BD}.ca WHERE ct=@ct", ct: opcao[:c])
      if bqres.empty?
        puts('CONTRATO NAO EXISTE')
        return
      end

      @bqres = bqres.first
      puts(' crontrato       data    renda')
      puts(format('%<ct>10s %<dc>10s %<vr>8.2f', ct: bqres[:ct], dc: bqres[:dc].strftime(DF), vr: bqres[:vr]))
      return unless opcao[:t]

      sql("SELECT * FROM #{BD}.gca WHERE ct=@ct order by ano desc,cnt desc", ct: opcao[:c])
      puts("\n      data  ano cnt dias")
      bqres.each { |l| puts(format('%<dl>10s %<a>4d %<c>3d %<d>4d', dl: l[:dl].strftime(DF), a: l[:ano], c: l[:cnt], d: l[:dias])) }
    end

    # cria contrato arrendamento no bigquery
    # @return [Big] acesso a base dados abank no bigquery
    def ct_cria
      if sql("SELECT ct FROM #{BD}.ca WHERE ct=@ct", ct: opcao[:c]).empty?
        if valid_dc?
          dml("insert #{BD}.re select @ct,EXTRACT(YEAR FROM DATE(@dc)),0,DATE(@dc),0", ct: opcao[:c], dc: opcao[:d])
        else
          dml("insert #{BD}.re select ct,EXTRACT(YEAR FROM DATE_TRUNC(dl,MONTH)),0,DATE_TRUNC(dl,MONTH),0 from #{BD}.mv where ct=@ct order by dl limit 1", ct: opcao[:c])
        end
        puts("CONTRATO #{opcao[:c]} #{bqnrs.zero? ? 'NAO ' : ''}INSERIDO")
      else
        @bqnrs = 1
        puts('CONTRATO JA EXISTE')
      end
      @ctlct = [{ct: opcao[:c]}]
      return unless bqnrs.positive? && opcao[:t]

      # processa rendas associadas ao contrato arrendamento
      ct_dados.re_insert
    end

    # (see CLI#apagact)
    def ct_apaga
      @ctlct = [{ct: opcao[:c]}]
      re_delete_dml
    end

    # optem lista dados contrato arrendamento (inclui lista movimentos novos)
    # @return [Big] acesso a base dados abank no bigquery
    def ct_dados
      ctlct.map! do |ctr|
        opcao[:c] = ctr[:ct]
        lre = sql("select * from #{BD}.glr where ct=@ct order by ano desc,cnt desc limit 1", ct: opcao[:c]).first
        lre[:dl] -= 1 if lre[:cnt].zero?
        ctr.merge(lre, mv: sql("select * from #{BD}.gmr where ct=@ct and dv>=@ud order by 1,2", ct: opcao[:c], ud: lre[:dl] + 1))
      end
      self
    end

    # insere rendas associadas a lista contratos arrendamento no bigquery
    def re_insert
      @ctpos = 0
      vls = ct_rendas.join(',')
      if vls.empty?
        puts('NAO EXISTEM RENDAS NOVAS')
      else
        dml("insert #{BD}.re VALUES#{vls}")
        puts("RENDAS #{cta.join(',')} CRIADAS #{bqnrs}")
      end
    end

    # @return [String] sql inicio contrato arrendamento sem movimentos
    # (see CLI#recriare)
    def re_atualiza
      # [re]cria rendas novas/todas dos contratos ativos
      @ctlct = sql("select ct from #{BD}.ca")
      re_apaga.ct_dados.re_insert
    end

    private

    # @param [Array<Hash>] acc classificadores (cc)
    # @return [Array<String]> array palavras formatadas
    def fplo(acc)
      acc.map { |p| "#{p[:p1]}#{'[' + p[:t1] + ']' unless p[:t1].to_s.empty?}#{'[2:' + p[:p2] + ']' unless p[:p2].to_s.empty?}" }
    end

    # @param [Array<String>] apl palavras
    # @param [String] ini pre-texto final
    # @return [String] palavras juntas filtradas para mostrar
    def fpls(apl, ini = '')
      return '' if apl.empty?

      "#{ini}#{apl.join(' ')}"
    end

    # @param [String] pla palavra
    # @return [String] palavra formatada para insert
    def fpli(pla)
      pla.to_s.empty? ? 'null' : "'#{pla}'"
    end

    # @return [Google::Cloud::Bigquery] API bigquery
    def bqapi
      @bqapi ||= Google::Cloud::Bigquery.new
    end

    # @return [Integer] numero conta apagar movimentos
    def contadel
      @contadel ||= opcao[:n] > 3 ? opcao[:n] : 0
    end

    # @return [String] movimentos a apagar (keysin.mv)
    def mvkys
      @mvkys ||= opcao[:k].to_s.scan(/[-+]?\d+/).map(&:to_i)
    end

    # @return [Boolean] data contrato arrendamento valida?
    def valid_dc?
      s = opcao[:d].to_s.strip
      return false unless s.length.positive?

      d = Date.parse(s)
      opcao[:d] = d.iso8601
      Date.valid_date?(d.year, d.month, d.day)
    rescue StandardError
      false
    end

    # apaga rendas da lista de contrato arrendamento
    # @return [Big] acesso a base dados abank no bigquery
    def re_apaga
      return self if ctlct.empty?

      # para nao apagar contrato arrendamento - somente as rendas
      opcao[:t] = false
      re_delete_dml
      self
    end

    # @example sem dados movimentos
    #   [{ ct: 'r03000' }, ...]
    # @example com dados movimentos
    #   [{ct: 'r03000', dc: '2020-03-01', ano: 2020, cnt: 0, dl: '2020-03-01', mv: [{dl: '2020-03-02', vl: 30}, ...]  }]
    # @return [Array<Hash>] lista dados contrato arrendamento (inclui lista movimentos novos)
    def ctlct
      @ctlct ||= []
    end

    # @return [Array<String>] lista de contratos arrendamento
    def cta
      ctlct.map { |c| c[:ct] }
    end

    # @param [Array] are lista rendas novas atual
    # @return [Array<String>] lista rendas novas duma lista contratos arrendamento
    def ct_rendas
      lre = []
      ctlct.each do
        @mvpos = 0
        lre += re_rendas
        @ctpos += 1
      end
      lre
    end

    # @return [Array<String>] lista rendas novas dum contrato arrendamento
    def re_rendas
      lre = []
      while mvpos < re_atual[:mv].size && re_saldo_mv?
        lre << re_nova_renda
        @mvpos += 1 unless re_saldo_mv?
      end
      lre
    end

    # @return [String] renda formatada (values.re)
    def re_nova_renda
      re_proximos_dados
      "('#{re_atual[:ct]}',#{ano},#{cnt},'#{re_atual_mv[:dl].iso8601}',#{dias})"
    end

    # @return [Hash] dados contrato arrendamento (inclui lista movimentos novos)
    def re_proximos_dados
      # valor renda paga retirada do movimento
      re_atual_mv[:vl] -= re_atual[:vr]
      dre = cnt.zero? ? Date.new(re_atual[:dc].year, re_atual[:dc].month, 1) : Date.new(ano, cnt, 1).next_month
      re_atual.merge!(ano: dre.year, cnt: dre.month)
    end

    # apaga movimentos no bigquery
    def mv_delete_dml
      if contadel.zero?
        dml("delete from #{BD}.mv where #{BD}.ky(dl,dv,ds,vl,nc,ex) IN UNNEST(@kys)", kys: mvkys)
      else
        dml("delete from #{BD}.mv where nc=@nc", nc: contadel)
      end
      puts("MOVIMENTOS APAGADOS #{bqnrs}")
    end

    # apaga rendas da lista de contratos arrendamento
    def re_delete_dml
      dml("delete from #{BD}.re where ct IN UNNEST(@cts)#{' and cnt>0' unless opcao[:t]}", cts: cta)
      puts("RENDAS #{cta.join(',')} APAGADAS #{bqnrs}")
    end

    # @return [Boolean] movimento com saldo suficiente?
    def re_saldo_mv?
      re_atual_mv[:vl] >= re_atual[:vr]
    end

    # @return [Hash] dados contrato arrendamento atual (inclui lista movimentos novos)
    def re_atual
      ctlct[ctpos]
    end

    # @return [Hash] movimento atual contrato arrendamento
    def re_atual_mv
      re_atual[:mv][mvpos]
    end

    # @return [Integer] ano da renda
    def ano
      re_atual[:ano]
    end

    # @return [Integer] numero da renda (0-12)
    def cnt
      re_atual[:cnt]
    end

    # @return [Integer] dias atraso/antecipo neste pagamento renda
    def dias
      re_atual_mv[:dl].mjd - Date.new(ano, cnt, 1).mjd
    end

    # cria job bigquery & verifica execucao
    # @param [String] cmd comando a executar
    # @param [Hash] prm parâmetros para a query
    # @return [Boolean] job ok?
    def job?(cmd, prm = {})
      @job = bqapi.query_job(cmd, params: prm, priority: 'BATCH')
      job.wait_until_done!
      return true unless job.failed?

      puts("BigQuery: #{job.error['message']}\n#{cmd}")
      false
    end

    # executa Structured Query Language (SQL) no bigquery
    # @param [String] cmd comando SQL a executar
    # @param [Hash] prm parâmetros para a query
    # @return [Google::Cloud::Bigquery::Data] resultado do SQL
    def sql(cmd, prm = {})
      @bqres = job?(cmd, prm) ? job.data : []
    end

    # executa Data Manipulation Language (DML) no bigquery
    # @param [String] cmd comando DML a executar
    # @param [Hash] prm parâmetros para a query
    # @return [Integer] numero linhas afetadas
    def dml(cmd, prm = {})
      @bqnrs = job?(cmd, prm) ? job.num_dml_affected_rows.to_i : 0
    end

    # executa Stored Procedure (STP) no bigquery
    # @param [String] cmd comando STP a executar
    # @return [Integer] numero rows afetadas pelo STP
    def stp(cmd)
      # last command STP=SELECT @@row_count AS rows_affected;
      @bqnrs = job?(cmd) ? job.data.first[:rows_affected].to_i : 0
    end
  end
end
